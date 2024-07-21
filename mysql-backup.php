<?php

/**
 * example.
 * 
 * mysqldump full backup.
 * php /mysql-backup.php --dir="/backup/" --action=full-archive --mode=dump --set-gtid-purged=OFF
 * 
 * mysqldump inc backup.
 * php /mysql-backup.php --dir="/backup/" --action=inc-archive --mode=dump --skip-full-unready
 * 
 * mysqldump recover backup
 * php /mysql-backup.php --dir="/backup/20240721" --action=recover
 * 
 * mysql shell full backup
 * php /mysql-backup.php --dir="/backup/" --action=full-archive --mode=mysqlsh
 * 
 * mysql shell recover backup
 * php /mysql-backup.php --dir="/backup/20240721/" --mode=mysqlsh --action=recover --reset-progress --redirect-primary
 */

const BINLOG_FILENAME = 'binlog_index';
const LOG_FILENAME = 'bak.log';
const FULL_BACKUP_SUFFIX = 'full_backup';
const INCREMENTAL_BACKUP_SUFFIX = 'inc_bak';
const ACTION_DEFAULT = 'full';
const MODE_DEFAULT = 'dump';

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
            print('❌ connect to mysql failed: ' . $e->getMessage());
            exit(1);
        }

        $dbInstance->query("SET NAMES 'utf8mb4'");
    
        // set the default fetch mode
        $dbInstance->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC);
        $dbInstance->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
    }

    return $dbInstance;
}

/**
 * mysqldump or inc dump
 * @param string $backupDir
 * @param bool $fullBackup
 * @return void
 */
function mysqlDump(string $backupDir, bool $fullBackup = false, bool $skipFullBackupUnReady = false, array $dumpExtra = [])
{
    // get mysql version
    $db = getMysqlDbInstance();
    $stmt = $db->query('SELECT VERSION()');
    $version = $stmt->fetchColumn();
    if (!$version) {
        print("❌ Mysql connected fail.\n");
    }

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
            "mysqldump -u%s -p%s -h%s -P%s --output-as-version=SERVER --single-transaction --quick --source-data=2 --all-databases %s > %s",
            MYSQL_USER,
            MYSQL_PASSWORD,
            MYSQL_HOST,
            MYSQL_PORT,
            implode(' ', $dumpExtra),
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
        if ($skipFullBackupUnReady) {
            print("⌛️ Skip full backup unready. later again.\n");
            exit(0);
        }

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
        print("⌛️ No binlog file need to backup. later again.\n");
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
function recoverMysqlDump(string $backupDir)
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

/**
 * backup mysql by mysqlsh
 *
 * @return void
 */
function mysqlShellDump(string $backupDir, bool $primary = false) 
{
    $backupDir = rtrim($backupDir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
    $user = MYSQL_USER;
    $password = MYSQL_PASSWORD;
    $host = MYSQL_HOST;
    $port = MYSQL_PORT;

    // clear dir.
    foreach(glob($backupDir . '*') ?: [] as $item) {
        if (is_dir($item)) {
            rmdir($item);
            continue;
        }
        unlink($item);
    }

    $nodeSelectStr = $primary ? '--redirect-primary' : '--redirect-secondary';
    exec(
        "mysqlsh --cluster $nodeSelectStr -h$host -u$user -p$password -P$port --js -e \"util.dumpInstance('$backupDir')\"",
        $_,
        $returnCode
    );

    if ((int) $returnCode !== 0 || !is_file($backupDir . '@.json')) {
        print("❌ mysql shell backup failed. \n");
        exit(1);
    }

    print("✅ mysql shell backup succeed. \n");
    exit(0);
}

/**
 * mysql shell dump recover.
 * @return void
 */
function mysqlShellDumpRecover(string $backupDir, bool $resetProgress = false)
{
    $user = MYSQL_USER;
    $password = MYSQL_PASSWORD;
    $host = MYSQL_HOST;
    $port = MYSQL_PORT;

    $resetProgressStr = $resetProgress ? 'true' : 'false';
    exec(
        "mysqlsh --cluster --redirect-primary -h$host -u$user -p$password -P$port --js -e \"util.loadDump('$backupDir', {'resetProgress': $resetProgressStr})\"",
        $_,
        $returnCode
    );

    if ((int) $returnCode !== 0) {
        print("❌ mysql shell backup recover failed. \n");
        exit(1);
    }

    print("✅ mysql shell recover succeed. \n");
    exit(0);
}

function dumpHelp() {
    printf(<<<EOL
    Usage:
        --dir <target directory>, *required
        --action <action> enum:
            "full": create a backup file at this dir, default
            "full-archive": create a sub archive file at this dir and backup in this. 
                format as: dir/Ymd/
            "inc": create incremental backup file at this dir. unsupported when mode=mysqlsh
            "inc-archive": search the latest full-archive dir to create incremental backup file.
                unsupported when mode=mysqlsh
            "recover": recover mysql from this dir.
        --mode mysql backup mode. enum:
            "dump": create backup with mysqldump. default.
            "mysqlsh": create backup with mysql shell.
        --skip-full-unready <option> skip when full backup uncompleted. new archive dir is empty.
            only work with action=inc-archive or inc, work with mode=dump
        --set-gtid-purged <option> set gtid_purged when full backup. default: OFF.
            work with mode=dump
        --redirect-primary <option> switch to primary node in cluster. work with mode=mysqlsh
        --reset-progress <option> reset recover progress. work with mode=mysqlsh
        --user <mysql user> or Env: MYSQL_USER, *required
        --password <mysql password> or Env: MYSQL_PASSWORD, *required
        --port <mysql password> or Env MYSQL_PORT, default 3306
        --host <mysql password> or Env MYSQL_HOST, default localhost
        --help

    EOL);
}

// get php cli params
$options = getopt('', [
    'dir:', 
    'action:',
    'mode:',
    'user:',
    'password:',
    'port:',
    'host:',
    'set-gtid-purged:',
    'redirect-primary',
    'reset-progress',
    "skip-full-unready",
    'help'
]);

if (isset($options['help'])) {
    dumpHelp();
    exit(0);
}

$user = !empty($options['user']) ? $options['user'] : getenv('MYSQL_USER');
if (!$user) {
    print("❌ Missing mysql user.\n");
    dumpHelp();
    exit(1);
}
define('MYSQL_USER', $user);

$password = !empty($options['password']) ? $options['password'] : getenv('MYSQL_PASSWORD');
if (!$password) {
    print("❌ Missing mysql password.\n");
    dumpHelp();
    exit(1);
}
define('MYSQL_PASSWORD', $password);

$host = !empty($options['host']) ? $options['host'] : getenv('MYSQL_HOST');
define('MYSQL_HOST', $host ?: 'localhost');

$port = !empty($options['port']) ? $options['port'] : getenv('MYSQL_PORT');
define('MYSQL_PORT', $port ?: 3306);

$mode = $options['mode'] ?? MODE_DEFAULT;
if (!in_array($mode, ['dump', 'mysqlsh'])) {
    $mode = MODE_DEFAULT;
}

$action = $options['action'] ?? ACTION_DEFAULT;
if (
    !in_array($action, ['full', 'full-archive', 'inc', 'inc-archive', 'recover']) ||
    (
        $mode === 'mysqlsh' && 
        in_array($action, ['inc', 'inc-archive'])
    )
) {
    $action = ACTION_DEFAULT;
}

$dir = $options['dir'] ?? null;
if (!$dir || !is_dir($dir)) {
    printf("❌ Invalid backup directory: %s\n", $dir);
    dumpHelp();
    exit(1);
}
$dir = rtrim($dir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR;
if ($action === 'full-archive') {
    $dir .= date('Ymd') . DIRECTORY_SEPARATOR;
}

if ($mode === 'dump') {
    if ($action === 'inc-archive') {
        $dirs = glob($dir . '*', GLOB_ONLYDIR) ?: [];
        rsort($dirs);
        $foundArchive = false;
        foreach ($dirs as $searchedDir) {
            if (preg_match('/\d{8}/', basename($searchedDir))) {
                $dir = $searchedDir . DIRECTORY_SEPARATOR;
                $foundArchive = true;
                break;
            }
        }

        if (!$foundArchive) {
            print("❌ Could not find archive dir in $dir\n");
            exit(1);
        }

        print("🔍 Found archive dir in $dir\n");
    }

    if (str_starts_with($action, 'full')) {
        $setGtid = !empty($options['set-gtid-purged']) ? $options['set-gtid-purged'] : 'OFF';
        mysqlDump($dir, true, false, ["--set-gtid-purged=$setGtid"]);
    }
    elseif (str_starts_with($action, 'inc')) {
        mysqlDump(
            $dir,
            false,
            isset($options['skip-full-unready'])
        );
    }
    else {
        recoverMysqlDump($dir);
    }

    exit(0);
}

// mysqlsh mode.
if ($mode === 'mysqlsh') {
    if ($action === 'recover') {
        mysqlShellDumpRecover($dir, isset($options['reset-progress']));
        exit(0);
    }

    mysqlShellDump($dir, isset($options['redirect-primary']));
    exit(0);
}