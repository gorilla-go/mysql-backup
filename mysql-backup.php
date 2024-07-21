<?php

const MYSQL_USER = 'root';
const MYSQL_PASSWORD = 'root';
const MYSQL_HOST = '127.0.0.1';
const MYSQL_PORT = 3306;
const BINLOG_FILENAME = 'binlog_index';
const LOG_FILENAME = 'bak.log';
const FULL_BACKUP_SUFFIX = 'full_backup';
const INCREMENTAL_BACKUP_SUFFIX = 'inc_bak';

/**
 * @var \PDO|null $dbInstance
 */
$dbInstance = null;

/**
 * get mysql db instance.
 *
 * @return PDO
 */
function getMysqlDbInstance(): PDO
{
    global $dbInstance;
    if ($dbInstance === null) {
        try {
            $dbInstance = new \PDO(
                'mysql:dbname=mysql;host=' . MYSQL_HOST . ';port=' . MYSQL_PORT, 
                MYSQL_USER, 
                MYSQL_PASSWORD
            );
        } catch (\PDOException $e) {
            print($e->getMessage());
            exit(1);
        }

        $dbInstance->query("SET NAMES 'utf8mb4'");
    
        // set the default fetch mode
        $dbInstance->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC);
        $dbInstance->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
    }

    return $dbInstance;
}

function backupMysql(string $backupDir, bool $fullBackup = false)
{
    // get mysql version
    $db = getMysqlDbInstance();
    $stmt = $db->query('SELECT VERSION()');
    $version = $stmt->fetchColumn();
    $stmt->closeCursor();
    if (version_compare($version, '8.0.0', '<')) {
        print("❌ MySQL version $version is not supported, please use MySQL 8.0 or higher\n");
    }

    $backupDir = rtrim($backupDir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
    if (!is_dir($backupDir) && mkdir($backupDir, 0755, true) && !is_dir($backupDir)) {
        print("❌ Could not create backup directory: $backupDir\n");
        exit(1);
    }

    if ($fullBackup) {
        $backupFile = $backupDir . date('YmdHis') . '_' . FULL_BACKUP_SUFFIX . '.sql';

        // get mysqldump version
        exec("mysqldump -V", $output, $returnCode);
        if ($returnCode !== 0) {
            print("❌ Invalid mysqldump version\n");
            exit(1);
        }
        preg_match('/[0-9]\.[0-9]\.[0-9]+/', $output[0], $mysqldumpVersion);
        if (empty($mysqldumpVersion)) {
            print("❌ No found mysqldump version\n");
            exit(1);
        }
        $mysqldumpVersion = $mysqldumpVersion[0];
        if ($version !== $mysqldumpVersion) {
            print("❌ mysqldump version is $mysqldumpVersion, but mysql version is $version\n");
            exit(1);
        }

        // clear dir when full backup.
        foreach(glob($backupDir . '*') as $file) {
            unlink($file);
        }

        $command = sprintf(
            "mysqldump -u%s -p%s -h%s -P%s --single-transaction --quick --source-data=2 --all-databases > %s",
            MYSQL_USER,
            MYSQL_PASSWORD,
            MYSQL_HOST,
            MYSQL_PORT,
            $backupFile
        );

        printf("🤖 Full backup running...\n");
        exec($command, $output, $returnCode);
        if ($returnCode !== 0 || !is_file($backupFile)) {
            print("❌ Failed to create full backup.\n");
            exit(1);
        }

        // get master log file and position
        $backupFileFd = fopen($backupFile, 'r');
        $regex = "/CHANGE MASTER TO MASTER_LOG_FILE='(.+)', MASTER_LOG_POS=(\d+);/";
        $regexNewVersion = "/CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='(.+)', SOURCE_LOG_POS=(\d+);/";
        $maxScanLineNum = 300;
        $scanLines = 0;
        while (($line = fgets($backupFileFd)) !== false) {
            $scanLines++;
            if (preg_match($regex, $line, $matches) || preg_match($regexNewVersion, $line, $matches)) {
                $masterLogFile = trim($matches[1]);
                $masterLogPos = trim($matches[2]);

                file_put_contents($backupDir . BINLOG_FILENAME, $masterLogFile . ':' . $masterLogPos);
                file_put_contents(
                    $backupDir . LOG_FILENAME, 
                    sprintf(
                        "---- Full backup last file: %s, position: %s, at %s. \n\n",
                        $masterLogFile,
                        $masterLogPos,
                        date('c')
                    )
                );
                printf(
                    "✅ [%s] Full backup succeed. Master log file: %s, position: %s\n",
                    date('c'),
                    $masterLogFile,
                    $masterLogPos
                );
                exit(0);
            }

            if ($scanLines > $maxScanLineNum) {
                print("❌ Could not find master log file and position in backup file. 🔍 scan over the sql file.\n");
                exit(1);
            }
        }

        print("❌ Could not find master info (binlog file or position) in full backup file.\n");
        exit(1);
    }


    // incremental backup
    if (!is_file($backupDir . BINLOG_FILENAME)) {
        print("❌ Could not find binlog file in backup file. please do full backup first.\n");
        exit(1);
    }

    $binlog = file_get_contents($backupDir . BINLOG_FILENAME);
    $binlogArr = explode(':', $binlog);
    if (count($binlogArr) !== 2) {
        print("❌ Invalid binlog_index file.\n");
        exit(1);
    }
    [$binlogFile, $binlogPosition] = $binlogArr;
    
    $db = getMysqlDbInstance();
    $rows = $db->query('SHOW BINARY LOGS')->fetchAll() ?: [];

    $binlogFilesArr = [];
    foreach ($rows as $row) {
        if ($row['Log_name'] >= $binlogFile) {
            $binlogFilesArr[] = $row['Log_name'];
        }
    }

    sort($binlogFilesArr);
    $lastBinlogFile = end($binlogFilesArr);
    $firstBinlogFile = current($binlogFilesArr);
    $stmt = $db->query('SHOW BINARY LOG STATUS');
    $lastPositionRow = $stmt->fetch();
    $stmt->closeCursor();
    if ($lastBinlogFile !== $lastPositionRow['File']) {
        print("❌ binlog file is not the same as the last position find from mysql server.\n");
        exit(1);
    }
    $lastPosition = $lastPositionRow['Position'];

    if ((int) $lastPosition === (int) $binlogPosition && $lastBinlogFile === $binlogFile) {
        print("😴 No new binlog file need to backup. do it later.\n");
        exit;
    }

    $host = MYSQL_HOST;
    $port = MYSQL_PORT;
    $user = MYSQL_USER;
    $pass = MYSQL_PASSWORD;
    foreach ($binlogFilesArr as $binlogFileForBackup) {
        $incBakFileName = $backupDir . date('YmdHis') . '_' . INCREMENTAL_BACKUP_SUFFIX . '.sql';
        $commonConnStr = "mysqlbinlog -h$host -P$port -u$user -p$pass %s $binlogFileForBackup > $incBakFileName";
        
        $extraStr = ['--read-from-remote-server'];

        $isFirstBinlogFile = $binlogFileForBackup === $firstBinlogFile;
        if ($isFirstBinlogFile) {
            $extraStr[] = sprintf(
                '--start-position=%d',
                $binlogPosition
            );
        }
        
        $isLastBinlogFile = $binlogFileForBackup === $lastBinlogFile;
        if ($isLastBinlogFile) {
            $extraStr[] = sprintf(
                '--stop-position=%d',
                $lastPosition
            );
        }
        $command = sprintf(
            $commonConnStr,
            implode(' ', $extraStr)
        );

        printf("🤖 Incremental backup running...\n");
        exec($command, $_, $code);
        if ((int) $code !== 0 || !is_file($incBakFileName)) {
            print("❌ Incremental backup error\n");
            exit(1);
        }

        if ($isFirstBinlogFile) {
            file_put_contents(
                $backupDir . LOG_FILENAME, 
                sprintf(
                    "---- Start incremental backup from %s, position: %d, at %s\n",
                    $binlogFileForBackup,
                    $binlogPosition,
                    date('c')
                ),
                FILE_APPEND
            );
        }

        file_put_contents(
            $backupDir . LOG_FILENAME, 
            sprintf(
                "[%s] file: %s, position: %s, incremental backup: %s. \n",
                date('c'),
                $lastBinlogFile,
                $lastPosition,
                $incBakFileName
            ),
            FILE_APPEND
        );

        if ($isLastBinlogFile) {
            file_put_contents(
                $backupDir . LOG_FILENAME, 
                sprintf(
                    "---- End backup file: %s, position: %d, at %s\n\n",
                    $lastBinlogFile,
                    $lastPosition,
                    date('c')
                ),
                FILE_APPEND
            );
        }

        printf(
            "✅ [%s] Incremental backup done, latest binlog file: %s, position: %d\n",
            date('c'),
            $lastBinlogFile,
            $lastPosition
        );
    }

    // write the last position to the file
    file_put_contents($backupDir . BINLOG_FILENAME, $lastBinlogFile . ':' . $lastPosition);
}


/**
 * recover mysql from a backup.
 *
 * @param string $backupDir
 * @return void
 */
function recoverMysql(string $backupDir)
{
    $backupDir = rtrim($backupDir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
    $fileBackupFileSuffix = FULL_BACKUP_SUFFIX;
    $sqlFiles = glob($backupDir . "*_$fileBackupFileSuffix.sql");
    if (empty($sqlFiles)) {
        echo "❌ No backup file found\n";
        return;
    }

    $fullBackupName = $sqlFiles[0];
    print("✅ Recovering full backup file: $fullBackupName\n");

    $user = MYSQL_USER;
    $password = MYSQL_PASSWORD;
    $host = MYSQL_HOST;
    $port = MYSQL_PORT;
    exec(
        "mysql -u$user -p$password -h$host -P$port < $fullBackupName",
        $output,
        $returnCode
    );
    
    if ((int) $returnCode !== 0) {
        print("❌ Failed to recover full backup file.\n");
        exit(0);
    }

    $incBakSuffix = INCREMENTAL_BACKUP_SUFFIX;
    $incBakFiles = glob($backupDir . "*_$incBakSuffix.sql");
    if (count($incBakFiles) === 0) {
        print("✅ Recover succeed! no incremental backup file found.\n");
        exit(0);
    }

    sort($incBakFiles);
    foreach ($incBakFiles as $incBakFile) {
        printf(
            "🔥 Recovering incremental backup: %s\n", 
            $incBakFile
        );

        exec(
            "mysql -u$user -p$password -h$host -P$port < $incBakFile",
            $output,
            $returnCode
        );

        if ($returnCode !== 0) {
            print("❌ Recover failed! incremental backup file: $incBakFile\n");
            exit(1);
        }
    }

    print("✅ Recover success!\n");
}

backupMysql("/Users/yehua/Downloads/backup/", true);