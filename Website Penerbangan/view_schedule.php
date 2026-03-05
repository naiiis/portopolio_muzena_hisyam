<?php
$conn = new mysqli("localhost", "root", "", "flight_db");
if ($conn->connect_error) die("Koneksi gagal: " . $conn->connect_error);

// Ambil semua jadwal
$result = $conn->query("SELECT * FROM flight_schedule");

function isFlightOnDay($dos, $date) {
    $day = date('N', strtotime($date)); // 1-7 (Senin-Minggu)
    return strpos($dos, (string)$day) !== false;
}

echo "<h2>Jadwal Terbang</h2>";
while ($row = $result->fetch_assoc()) {
    echo "<h4>Nomor: {$row['nomor_penerbangan']} ({$row['rute_penerbangan']})</h4>";
    $start = new DateTime($row['start_date']);
    $end = new DateTime($row['end_date']);
    $end->modify('+1 day');

    $interval = new DateInterval('P1D');
    $daterange = new DatePeriod($start, $interval, $end);

    echo "<ul>";
    foreach ($daterange as $date) {
        $tgl = $date->format("Y-m-d");
        $status = isFlightOnDay($row['dos'], $tgl) ? "✅ ADA PENERBANGAN" : "❌ TIDAK ADA";
        echo "<li>$tgl → $status</li>";
    }
    echo "</ul>";
}
?>
