<?php
include 'simplexlsx.class.php';

$conn = new mysqli("localhost", "root", "", "flight_db");
if ($conn->connect_error) die("Koneksi gagal: " . $conn->connect_error);

if (isset($_FILES['file']['name'])) {
    if ($xlsx = SimpleXLSX::parse($_FILES['file']['tmp_name'])) {
        $rows = $xlsx->rows();

        foreach ($rows as $i => $row) {
            if ($i == 0) continue; // skip header
            list($rute, $tipe, $nomor, $etd, $eta, $dos, $frekuensi, $masa, $dep, $arr, $iata, $flight, $icao, $dep_icao, $arr_icao) = $row;

            // Ubah masa berlaku: 28 OKT 2024/24 MAR 2025 → jadi 2 tanggal
            $dates = explode("/", $masa);
            $start_date = convert_date(trim($dates[0]));
            $end_date = convert_date(trim($dates[1]));

            // Masukkan ke DB
            $stmt = $conn->prepare("INSERT INTO flight_schedule 
                (rute_penerbangan, tipe_pesawat, nomor_penerbangan, etd, eta, dos, frekuensi, start_date, end_date, dep, arr, iata_code, flight_number, icao_airline, dep_icao, arr_icao)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            $stmt->bind_param("ssssssssssssssss", $rute, $tipe, $nomor, $etd, $eta, $dos, $frekuensi, $start_date, $end_date, $dep, $arr, $iata, $flight, $icao, $dep_icao, $arr_icao);
            $stmt->execute();
        }
        echo "Berhasil upload dan simpan ke database. <a href='view_schedule.php'>Lihat Jadwal</a>";
    } else {
        echo SimpleXLSX::parseError();
    }
}

function convert_date($tgl) {
    $bulan = ["JAN"=>"01", "FEB"=>"02", "MAR"=>"03", "APR"=>"04", "MEI"=>"05", "MAY"=>"05", "JUN"=>"06", "JUL"=>"07", "AGU"=>"08", "AUG"=>"08", "SEP"=>"09", "OKT"=>"10", "OCT"=>"10", "NOV"=>"11", "DES"=>"12", "DEC"=>"12"];
    $parts = explode(" ", strtoupper($tgl));
    return "{$parts[2]}-{$bulan[$parts[1]]}-{$parts[0]}";
}
?>