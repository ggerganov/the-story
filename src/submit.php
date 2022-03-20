<?php

// parse GET parameters
$slot_raw   = $_GET['s'];
$input_raw  = $_GET['i'];
$userId_raw = $_GET['u'];

// sanitize and escape the input
// - slot must be a number
// - input must be a string, containing only lowercase letters
// - userId must be a 16-bit unsigned integer
$slot   = intval($slot_raw);
$input  = strtolower($input_raw);
$userId = intval($userId_raw);

if ($slot < 0) {
    $slot = null;
}

$regex = '/^[a-z]+$/';
if (!preg_match($regex, $input)) {
    $input = null;
}

if ($userId < 0 || $userId > 65535) {
    $userId = null;
}

if ($slot === null || $input === null || $userId === null || $slot != $slot_raw || $input != $input_raw) {
    $response = array(
        'error' => 1,
        'message' => 'Invalid input'
    );
    echo json_encode($response);
    exit;
}

// check if $input exists in file 'words-alpha.txt' using 'grep -Fx'
$command = 'grep -Fx ' . escapeshellarg($input) . ' words-alpha.txt';
$output = shell_exec($command);

if ($output === null) {
    $response = array(
        'error' => 1,
        'message' => 'Word not found'
    );
    echo json_encode($response);
    exit;
}

// get IP address
$ip = $_SERVER['REMOTE_ADDR'];

// get timestamp in seconds since epoch
$timestamp = time();

$uid = uniqid('', true);

// generate unique filename in folder 'pending'
$filename = './pending/t' . $uid;

// in the file, write space separated: timestamp, IP, slot, userId, input
$file = fopen($filename, 'w');
fwrite($file, $timestamp . ' ' . $ip . ' ' . $slot . ' ' . $userId . ' ' . $input);
fclose($file);

// atomically rename the file
rename($filename, './pending/s' . $uid);

$response = array(
    'error' => 0,
    'message' => 'Vote registered at '.$timestamp,
);
echo json_encode($response);

?>
