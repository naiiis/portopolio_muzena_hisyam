from flask import Flask, render_template, request, redirect, flash
from flask_mysqldb import MySQL
import pandas as pd, os, re
from datetime import datetime, time
from decimal import Decimal
from collections import defaultdict
import json

app = Flask(__name__)
app.secret_key = "secretkey"

app.config.update(
    MYSQL_HOST="localhost",
    MYSQL_USER="root",
    MYSQL_PASSWORD="",
    MYSQL_DB="flight_db2"
)
mysql = MySQL(app)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

clean = lambda t: re.sub(r"[\s./\-]", "", str(t).lower())

def to_time(v):
    if v is None or pd.isna(v): return None
    if isinstance(v, (datetime, pd.Timestamp)): return v.strftime("%H:%M:%S")
    if isinstance(v, time): return v.strftime("%H:%M:%S")
    if isinstance(v, (float, int)) and not isinstance(v, bool):
        if 100 <= v < 2400 and float(v).is_integer(): v = f"{int(v):04d}"
        else:
            secs = int(round(float(v) % 1 * 86400))
            return f"{secs//3600:02d}:{(secs%3600)//60:02d}:{secs%60:02d}"
    s = str(v).strip().replace("\u200b","").replace(".",":").replace("-",":")
    if re.fullmatch(r"\d{3,4}", s): s = f"{s[:-2]}:{s[-2:]}"
    for fmt in ("%H:%M:%S", "%H:%M"):
        try: return datetime.strptime(s, fmt).strftime("%H:%M:%S")
        except ValueError: continue
    return None

bulan = {m: f"{i:02d}" for i, m in enumerate(
    ["JAN","FEB","MAR","APR","MEI","JUN","JUL","AGU","SEP","OKT","NOV","DES"], 1)}
bulan.update({"AUG":"08","DEC":"12","OCT":"10","MAY":"05"})

def convert_date(txt):
    try:
        parts = txt.strip().upper().split()
        if len(parts) == 3:
            d, m, y = parts
            return f"{y}-{bulan.get(m[:3], '01')}-{d.zfill(2)}"
        return None
    except:
        return None

def to_date(x):
    if pd.isna(x): return None
    try: return pd.to_datetime(x).date()
    except: pass
    if isinstance(x, str):
        c = convert_date(x)
        if c:
            try: return pd.to_datetime(c).date()
            except: return None
    return None

def col_re(df, *pats):
    for pat in pats:
        r = re.compile(pat, re.I)
        for c in df.columns:
            if r.search(c): return c
    return None

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        f = request.files.get("file")
        tp = request.form.get("dataset_type")

        if not f or f.filename == "":
            flash("❌ File tidak ditemukan"); return redirect("/")

        path = os.path.join(UPLOAD_FOLDER, f.filename)
        f.save(path)

        try:
            preview = pd.read_excel(path, header=None, dtype=str)
            header_row = None

            if tp == 'planning':
                for i, row in preview.iterrows():
                    if any("rutepenerbangan" in clean(c) for c in row if pd.notna(c)):
                        header_row = i; break
            else:
                for i, row in preview.iterrows():
                    cells = [clean(c) for c in row if pd.notna(c)]
                    if {"tanggal", "acid", "areg"}.issubset(cells):
                        header_row = i; break

            if header_row is None:
                flash("❌ Header tidak ditemukan"); return redirect("/")

            df = pd.read_excel(path, header=header_row, dtype=str)
            df.columns = [clean(c) for c in df.columns]
            if "no" in df.columns:
                df = df.drop(columns=["no"])

            cur, ok = mysql.connection.cursor(), 0

            # Simpan metadata file
            cur.execute(
                "INSERT INTO uploaded_files (filename, upload_time, file_type) VALUES (%s, %s, %s)",
                (f.filename, datetime.now(), tp)
            )
            upload_id = cur.lastrowid

            if tp == "planning":
                rute = col_re(df, "rute.*penerbangan")
                nomor = col_re(df, "nomor.*penerbangan")
                dos = col_re(df, "^dos$")
                if not all([rute, nomor, dos]):
                    flash("❌ Kolom wajib planning hilang."); return redirect("/")

                etd, eta = col_re(df, "^etd$"), col_re(df, "^eta$")
                tipe, kap = col_re(df, "tipe.*pesawat"), col_re(df, "kapasitas.*pesawat")
                frek, masa = col_re(df, "frekuensi"), col_re(df, "masaberlaku")
                penerb, pengaj = col_re(df, "nomor.*penerbitan"), col_re(df, "tipe.*pengajuan")

                df = df.dropna(subset=[rute, nomor, dos])
                if penerb: df[penerb] = df[penerb].ffill()

                for idx, r in df.iterrows():
                    try:
                        dep, arr = (r[rute].split("-") + [None])[:2] if "-" in str(r[rute]) else (None, None)
                        code = str(r[nomor]).strip()
                        iata, fl = code[:2].upper(), code[2:]

                        sd, ed = None, None
                        if masa and isinstance(r.get(masa), str) and "/" in r[masa]:
                            a, b = r[masa].split("/")
                            sd_txt, ed_txt = a.strip(), b.strip()
                            sd = pd.to_datetime(convert_date(sd_txt)) if convert_date(sd_txt) else None
                            ed = pd.to_datetime(convert_date(ed_txt)) if convert_date(ed_txt) else None

                        start_date = sd.date() if pd.notna(sd) else None
                        end_date = ed.date() if pd.notna(ed) else None
                        if not start_date or not end_date: continue

                        d = start_date
                        while d <= end_date:
                            iso_dow = str((d.weekday() + 1))
                            if iso_dow in str(r[dos]):
                                cur.execute("""INSERT INTO main_prpp_generated(
                                    arrival, departure, tipe_pesawat, kapasitas_pesawat,
                                    iata_id, flight_number, etd, eta, dos, frekuensi,
                                    start_date, end_date, nomor_penerbitan, tipe_pengajuan,
                                    tanggal, dep_arr, upload_id
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                                    arr, dep, r.get(tipe), r.get(kap), iata, fl,
                                    to_time(r.get(etd)), to_time(r.get(eta)),
                                    str(r[dos]).zfill(7), r.get(frek),
                                    start_date, end_date,
                                    r.get(penerb), r.get(pengaj),
                                    d, "dep", upload_id
                                ))
                                ok += 1
                            d += pd.Timedelta(days=1)
                    except Exception as e:
                        print(f"[skip planning] baris ke-{idx}: {e}")

            else:
                rg = {
                    "tanggal": [r"^tanggal$"], "acid": [r"^acid$"], "areg": [r"a.*reg"], "atype": [r"a.*type"],
                    "adep": [r"^adep$"], "ades": [r"^ades$"], "eobt": [r"^eobt$"], "pushback": [r"^pushback$"],
                    "taxi": [r"^taxi$"], "deparrlocal": [r"dep.*arr.*local"], "atd": [r"^atd$"],
                    "eta": [r"^eta$"], "ata": [r"^ata$"], "riudep": [r"riu.*dep"], "riuarr": [r"riu.*arr"],
                    "parkingdep": [r"parking.*dep"], "parkingarr": [r"parking.*arr"], "pob": [r"^pob$"],
                    "remark": [r"^remark$"], "statusflight": [r"status.*flight"]
                }
                col = {k: col_re(df, *v) for k, v in rg.items()}
                if not col["tanggal"] or not col["acid"]:
                    flash("❌ Kolom tanggal / acid tidak ada."); return redirect("/")

                g = lambda row, key: row[col[key]] if col[key] in row and pd.notna(row[col[key]]) else None

                for _, r in df.iterrows():
                    try:
                        tgl = to_date(g(r, "tanggal"))
                        if not tgl: continue
                        pob = g(r, "pob"); pob = int(pob) if pob and str(pob).isdigit() else None
                        cur.execute("""INSERT INTO main_realisasi(
                            tanggal, acid, a_reg, a_type, adep, ades, eobt, pushback, taxi, dep_arr_local,
                            atd, eta, ata, riu_dep, riu_arr, parking_dep, parking_arr, pob, remark, status_flight,
                            upload_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (tgl, g(r, "acid"), g(r, "areg"), g(r, "atype"), g(r, "adep"), g(r, "ades"),
                             to_time(g(r, "eobt")), to_time(g(r, "pushback")), to_time(g(r, "taxi")),
                             g(r, "deparrlocal"), to_time(g(r, "atd")), to_time(g(r, "eta")), to_time(g(r, "ata")),
                             g(r, "riudep"), g(r, "riuarr"), g(r, "parkingdep"), g(r, "parkingarr"),
                             pob, g(r, "remark"), g(r, "statusflight"), upload_id))
                        ok += 1
                    except Exception as e:
                        print(f"[skip real] {e}")

            mysql.connection.commit()

            # === Prosedur SQL ===
            if tp == "planning":
                cur.execute("CALL konversi_kode_iata_ke_icao()")
            elif tp == "realisasi":
                cur.execute("SELECT DISTINCT tanggal FROM main_realisasi WHERE upload_id = %s", (upload_id,))
                tanggal_realisasi = set(row[0] for row in cur.fetchall())
                if tanggal_realisasi:
                    cur.execute("SELECT DISTINCT tanggal FROM main_prpp_generated WHERE tanggal IN %s", (tuple(tanggal_realisasi),))
                    tanggal_prpp = set(row[0] for row in cur.fetchall())
                    if tanggal_prpp:
                        cur.execute("CALL hitung_delay_ke_tabel_baru()")
                    else:
                        cur.execute("DELETE FROM main_realisasi WHERE upload_id = %s", (upload_id,))
                        cur.execute("DELETE FROM uploaded_files WHERE id = %s", (upload_id,))
                        mysql.connection.commit()
                        flash("❌ File PRPP untuk season ini belum di-upload.")
                        return redirect("/")

            cur.close()
            flash(f"✅ {ok} baris tersimpan ke {'main_prpp_generated' if tp=='planning' else 'main_realisasi'}")

        except Exception as e:
            flash(f"❌ Gagal: {e}")
        finally:
            if os.path.exists(path):
                os.remove(path)
        return redirect("/")

    cur = mysql.connection.cursor()
    cur.execute("SELECT id, filename, upload_time, file_type FROM uploaded_files ORDER BY upload_time DESC")
    files = cur.fetchall()
    cur.close()
    return render_template("upload.html", files=files)

@app.route("/delete/<int:file_id>", methods=["POST"])
def delete_file(file_id):
    cur = mysql.connection.cursor()
    cur.execute("SELECT filename, file_type FROM uploaded_files WHERE id = %s", (file_id,))
    result = cur.fetchone()

    if result:
        filename, file_type = result

        if file_type == "realisasi":
            cur.execute("SELECT id FROM main_realisasi WHERE upload_id = %s", (file_id,))
            real_ids = [r[0] for r in cur.fetchall()]
            if real_ids:
                format_str = ",".join(["%s"] * len(real_ids))
                cur.execute(f"DELETE FROM hasil_delay_pushback WHERE id_realisasi IN ({format_str})", tuple(real_ids))
            cur.execute("DELETE FROM main_realisasi WHERE upload_id = %s", (file_id,))

        elif file_type == "planning":
            # Cek apakah masih ada realisasi yang terkait
            cur.execute("SELECT COUNT(*) FROM hasil_delay_pushback WHERE id_prpp IN (SELECT id FROM main_prpp_generated WHERE upload_id = %s)", (file_id,))
            count_delay = cur.fetchone()[0]
            if count_delay > 0:
                flash("❌ Gagal menghapus: Hapus data realisasi terlebih dahulu.")
                cur.close()
                return redirect("/")

            cur.execute("SELECT id FROM main_prpp_generated WHERE upload_id = %s", (file_id,))
            prpp_ids = [r[0] for r in cur.fetchall()]
            if prpp_ids:
                format_str = ",".join(["%s"] * len(prpp_ids))
                cur.execute(f"DELETE FROM hasil_delay_pushback WHERE id_prpp IN ({format_str})", tuple(prpp_ids))
            cur.execute("DELETE FROM main_prpp_generated WHERE upload_id = %s", (file_id,))

        cur.execute("DELETE FROM uploaded_files WHERE id = %s", (file_id,))
        mysql.connection.commit()

        file_path = os.path.join(UPLOAD_FOLDER, filename)
        if os.path.exists(file_path):
            os.remove(file_path)

        flash(f"🗑️ File '{filename}' dan datanya berhasil dihapus.")
    else:
        flash("❌ File tidak ditemukan.")
    cur.close()
    return redirect("/")

@app.route("/delay-stats")
def delay_stats():
    bulan = request.args.get("bulan")
    view = request.args.get("view", "harian")
    selected_icao = request.args.get("icao")
    selected_tujuan = request.args.get("tujuan")
    jenis = request.args.get("jenis", "domestik")
    cur = mysql.connection.cursor()

    # ───── Filter Jenis ─────
    if jenis == "domestik":
        filt_jenis = "AND (mp.arrival_icao LIKE 'WA%' OR mp.arrival_icao LIKE 'WI%')"
        filt_jenis_raw = "AND (arrival_icao LIKE 'WA%' OR arrival_icao LIKE 'WI%')"
    elif jenis == "internasional":
        filt_jenis = "AND (mp.arrival_icao NOT LIKE 'WA%' AND mp.arrival_icao NOT LIKE 'WI%')"
        filt_jenis_raw = "AND (arrival_icao NOT LIKE 'WA%' AND arrival_icao NOT LIKE 'WI%')"
    else:
        filt_jenis = ""
        filt_jenis_raw = ""

    # ───── Available Filters ─────
    cur.execute("""
        SELECT DISTINCT DATE_FORMAT(mp.tanggal,'%Y-%m') AS b
        FROM hasil_delay_pushback op
        JOIN main_prpp_generated mp ON mp.id = op.id_prpp
        ORDER BY b DESC
    """)
    available_months = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT ICAO_CODE, Maskapai FROM corpus_airline")
    airline_map = dict(cur.fetchall())

    cur.execute("SELECT ICAO_CODE, CONCAT(NAME_Airport, ' - ', LOCATION) FROM corpus_airport")
    airport_map = dict(cur.fetchall())

    cur.execute(f"""
        SELECT DISTINCT airline_icao FROM main_prpp_generated
        WHERE departure_icao = 'WARR' {filt_jenis_raw}
        AND airline_icao IS NOT NULL
        ORDER BY airline_icao
    """)
    available_icaos = [(icao, airline_map.get(icao, icao)) for icao, in cur.fetchall()]

    cur.execute(f"""
        SELECT DISTINCT arrival_icao FROM main_prpp_generated
        WHERE departure_icao = 'WARR' {filt_jenis_raw}
        AND arrival_icao IS NOT NULL
        ORDER BY arrival_icao
    """)
    available_destinations = [(icao, airport_map.get(icao, icao)) for icao, in cur.fetchall()]

    # ───── Filter Lanjutan ─────
    filt_bulan = f"AND DATE_FORMAT(mp.tanggal, '%Y-%m') = '{bulan}'" if bulan else ""
    filt_icao = f"AND mp.airline_icao = '{selected_icao}'" if selected_icao else ""
    filt_tujuan = f"AND mp.arrival_icao = '{selected_tujuan}'" if selected_tujuan else ""

    group_by = "YEARWEEK(mp.tanggal, 3)" if view == "mingguan" else "DATE(mp.tanggal)"
    label_col = "minggu" if view == "mingguan" else "tanggal"

    # ───── Grafik Kategori ─────
    cur.execute(f"""
        SELECT mp.airline_icao, {group_by} AS {label_col},
               SUM(CASE WHEN op.delay_minutes <= 15 THEN 1 ELSE 0 END) AS ontime,
               SUM(CASE WHEN op.delay_minutes BETWEEN 16 AND 30 THEN 1 ELSE 0 END) AS delay1,
               SUM(CASE WHEN op.delay_minutes >= 31 THEN 1 ELSE 0 END) AS delay2
        FROM hasil_delay_pushback op
        JOIN main_prpp_generated mp ON mp.id = op.id_prpp
        WHERE mp.departure_icao = 'WARR'
        {filt_jenis} {filt_bulan} {filt_icao} {filt_tujuan}
        GROUP BY mp.airline_icao, {label_col}
        ORDER BY {label_col}, mp.airline_icao
    """)
    rows = cur.fetchall()

    chart_data = defaultdict(lambda: {"ontime": 0, "delay1": 0, "delay2": 0})
    maskapai_aktif = set()

    for icao, _, ot, d1, d2 in rows:
        nama = airline_map.get(icao, icao)
        chart_data[nama]["ontime"] += int(ot)
        chart_data[nama]["delay1"] += int(d1)
        chart_data[nama]["delay2"] += int(d2)
        maskapai_aktif.add(nama)

    total_ontime = sum(chart_data[n]["ontime"] for n in maskapai_aktif)
    total_delay1 = sum(chart_data[n]["delay1"] for n in maskapai_aktif)
    total_delay2 = sum(chart_data[n]["delay2"] for n in maskapai_aktif)
    total_flight = total_ontime + total_delay1 + total_delay2

    def persen(x): return round(100 * x / total_flight, 1) if total_flight else 0

    summary_box = {
        "ontime": {"jumlah": total_ontime, "persen": persen(total_ontime)},
        "delay1": {"jumlah": total_delay1, "persen": persen(total_delay1)},
        "delay2": {"jumlah": total_delay2, "persen": persen(total_delay2)},
    }

    # ───── Timeline Menit Delay ─────
    cur.execute(f"""
        SELECT mp.airline_icao, {group_by} AS {label_col}, SUM(op.delay_minutes)
        FROM hasil_delay_pushback op
        JOIN main_prpp_generated mp ON mp.id = op.id_prpp
        WHERE op.delay_minutes > 15
        AND mp.departure_icao = 'WARR'
        {filt_jenis} {filt_bulan} {filt_icao} {filt_tujuan}
        GROUP BY mp.airline_icao, {label_col}
        ORDER BY {label_col}, mp.airline_icao
    """)
    timeline_rows = cur.fetchall()

    timeline = defaultdict(lambda: {"label": "", "data": {}})
    label_set = set()
    for icao, label, minutes in timeline_rows:
        nama = airline_map.get(icao, icao)
        label_str = str(label)
        label_set.add(label_str)
        timeline[nama]["label"] = nama
        timeline[nama]["data"][label_str] = int(minutes)

    date_labels = sorted(label_set, key=lambda x: int(x) if x.isdigit() else x)
    for nama in list(timeline):
        timeline[nama]["data"] = [timeline[nama]["data"].get(l, 0) for l in date_labels]

    timeline["__TOTAL__"] = {"label": "Total Semua Maskapai", "data": []}
    for i, label in enumerate(date_labels):
        total_menit = sum(timeline[nama]["data"][i] for nama in maskapai_aktif if nama in timeline)
        timeline["__TOTAL__"]["data"].append(total_menit)

    summary = {nama: sum(timeline[nama]["data"]) for nama in maskapai_aktif if nama in timeline}
    total_delay = sum(summary.values())

    # ───── Total Delay Keseluruhan ─────
    cur.execute(f"""
        SELECT SUM(op.delay_minutes)
        FROM hasil_delay_pushback op
        JOIN main_prpp_generated mp ON mp.id = op.id_prpp
        WHERE op.delay_minutes > 15
        AND mp.departure_icao = 'WARR'
        {filt_jenis} {filt_bulan} {filt_icao} {filt_tujuan}
    """)
    total_delay_semua = cur.fetchone()[0] or 0

    # ───── Preview Data ─────
    cur.execute(f"""
        SELECT mp.tanggal, mp.flight_number, mp.airline_icao, mp.etd, mr.pushback, op.delay_minutes
        FROM hasil_delay_pushback op
        JOIN main_prpp_generated mp ON mp.id = op.id_prpp
        JOIN main_realisasi mr ON mr.id = op.id_realisasi
        WHERE mp.departure_icao = 'WARR'
        {filt_jenis} {filt_bulan} {filt_icao} {filt_tujuan}
        ORDER BY mp.tanggal DESC
    """)
    rows = cur.fetchall()

    preview_rows = []
    for tgl, flt, icao, etd, pb, delay in rows:
        if delay <= 15:
            ket = "Ontime"
        elif delay <= 30:
            ket = "Delay 1"
        else:
            ket = "Delay 2"
        preview_rows.append({
            "tanggal": tgl.strftime("%Y-%m-%d") if isinstance(tgl, datetime) else tgl,
            "flight_number": flt,
            "maskapai": airline_map.get(icao, icao),
            "etd": etd,
            "pushback": pb,
            "delay_minutes": delay,  # ✅ ini dia yang ditambahkan
            "keterangan": ket
        })


    cur.close()

    return render_template("delay_rate.html",
        available_months=available_months,
        available_icaos=available_icaos,
        available_destinations=available_destinations,
        selected_month=bulan,
        selected_icao=selected_icao,
        selected_destination=selected_tujuan,
        view=view,
        jenis=jenis,
        chart_data={k: chart_data[k] for k in maskapai_aktif},
        summary=summary,
        total_delay=total_delay,
        total_delay_semua=total_delay_semua,
        timeline={k: timeline[k] for k in maskapai_aktif} | {"__TOTAL__": timeline["__TOTAL__"]},
        date_labels=date_labels,
        summary_box=summary_box,
        preview_rows=preview_rows
    )

@app.route("/delay-rate-taxi")
def delay_rate_taxi():
    bulan = request.args.get("bulan")
    selected_kode = request.args.get("icao")
    selected_tujuan = request.args.get("tujuan")
    jenis = request.args.get("jenis", "")
    cur = mysql.connection.cursor()

    # Filter jenis
    if jenis == "domestik":
        filt_jenis = "AND (mr.ades LIKE 'WA%' OR mr.ades LIKE 'WI%')"
    elif jenis == "internasional":
        filt_jenis = "AND (mr.ades NOT LIKE 'WA%' AND mr.ades NOT LIKE 'WI%')"
    else:
        filt_jenis = ""

    # Bulan tersedia
    cur.execute("""
        SELECT DISTINCT DATE_FORMAT(mr.tanggal, '%Y-%m') AS b
        FROM main_realisasi mr
        WHERE mr.pushback IS NOT NULL AND mr.atd IS NOT NULL
        ORDER BY b DESC
    """)
    available_months = [r[0] for r in cur.fetchall()]

    # Daftar maskapai unik dari acid prefix
    cur.execute(f"""
        SELECT DISTINCT LEFT(mr.acid, 3) AS kode, ca.Maskapai
        FROM main_realisasi mr
        JOIN corpus_airline ca ON LEFT(mr.acid, 3) = ca.ICAO_CODE
        WHERE mr.pushback IS NOT NULL AND mr.atd IS NOT NULL AND mr.adep = 'WARR' {filt_jenis}
        ORDER BY ca.Maskapai
    """)
    available_icaos = [(r[0], r[1]) for r in cur.fetchall()]

    # Tujuan tersedia
    cur.execute(f"""
        SELECT DISTINCT mr.ades
        FROM main_realisasi mr
        WHERE mr.pushback IS NOT NULL AND mr.atd IS NOT NULL AND mr.adep = 'WARR' {filt_jenis}
        ORDER BY mr.ades
    """)
    available_destinations = [(r[0], r[0]) for r in cur.fetchall()]

    # Filter tambahan
    filt_bulan = f"AND DATE_FORMAT(mr.tanggal, '%Y-%m') = '{bulan}'" if bulan else ""
    filt_kode = f"AND LEFT(mr.acid, 3) = '{selected_kode}'" if selected_kode else ""
    filt_tujuan = f"AND mr.ades = '{selected_tujuan}'" if selected_tujuan else ""


    # Query untuk total dan delay taxi-out
    cur.execute(f"""
        SELECT ca.Maskapai,
               SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, mr.pushback, mr.atd) <= 15 THEN 1 ELSE 0 END) AS ontime,
               SUM(CASE WHEN TIMESTAMPDIFF(MINUTE, mr.pushback, mr.atd) > 15 THEN 1 ELSE 0 END) AS delay,
               AVG(TIMESTAMPDIFF(MINUTE, mr.pushback, mr.atd)) AS avg_taxi
        FROM main_realisasi mr
        JOIN corpus_airline ca ON LEFT(mr.acid, 3) = ca.ICAO_CODE
        WHERE mr.pushback IS NOT NULL AND mr.atd IS NOT NULL AND mr.adep = 'WARR'
              {filt_jenis} {filt_bulan} {filt_kode} {filt_tujuan}
        GROUP BY ca.Maskapai
    """)
    rows = cur.fetchall()

    total_ontime = sum(int(r[1]) for r in rows)
    total_delay = sum(int(r[2]) for r in rows)
    total_flight = total_ontime + total_delay
    avg_taxi = round(sum(r[3] * (r[1] + r[2]) for r in rows) / total_flight, 1) if total_flight else 0

    def persen(x): return round(100 * x / total_flight, 1) if total_flight else 0

    cur.close()

    return render_template("delay_taxi_rate.html",
        available_months=available_months,
        available_icaos=available_icaos,
        available_destinations=available_destinations,
        selected_month=bulan,
        selected_icao=selected_kode,
        selected_destination=selected_tujuan,
        jenis=jenis,
        total_flight=total_flight,
        ontime=total_ontime,
        delay=total_delay,
        ontime_rate=persen(total_ontime),
        delay_rate=persen(total_delay),
        avg_taxi=avg_taxi
    )
# ── route utama ──────────────────────────────────────────
@app.route("/flight-compare")
def flight_compare():
    cur = mysql.connection.cursor()

    start_s = request.args.get("start")
    end_s = request.args.get("end")
    selected_airline = request.args.get("airline")
    selected_route = request.args.get("route")
    selected_departure = request.args.get("departure")
    selected_arrival = request.args.get("arrival")
    jenis = request.args.get("jenis", "semua")

    today = pd.Timestamp.today().normalize()
    start = pd.to_datetime(start_s) if start_s else today - pd.Timedelta(days=29)
    end = pd.to_datetime(end_s) if end_s else today

    # Master maskapai
    cur.execute("SELECT iata_code, icao_code, Maskapai FROM corpus_airline")
    airline_rows = cur.fetchall()
    iata_to_name = {r[0].upper(): r[2] for r in airline_rows}
    name_to_iata = {r[2]: r[0].upper() for r in airline_rows}
    icao_to_iata = {r[1].upper(): r[0].upper() for r in airline_rows}
    all_iata_ids = set(iata_to_name.keys())

    # Master bandara
    cur.execute("SELECT ICAO_CODE, NAME_Airport, LOCATION FROM corpus_airport")
    airport_rows = cur.fetchall()
    icao_to_nama_lokasi = {
        row[0]: f"{row[1]} ({row[2]})" for row in airport_rows if row[0] and row[1]
    }
    nama_lokasi_to_icao = {v: k for k, v in icao_to_nama_lokasi.items()}

    # Realisasi
    cur.execute("SELECT tanggal, acid, adep, ades, status_flight FROM main_realisasi")
    df_real = pd.DataFrame(cur.fetchall(), columns=["tanggal", "acid", "adep", "ades", "status_flight"])

    if not df_real.empty:
        df_real["tanggal"] = pd.to_datetime(df_real["tanggal"])
        df_real["acid"] = df_real["acid"].fillna("")
        df_real["adep"] = df_real["adep"].fillna("")
        df_real["ades"] = df_real["ades"].fillna("")
        df_real["status_flight"] = df_real["status_flight"].fillna("").str.upper()
        df_real["icao_code"] = df_real["acid"].str[:3].str.upper()
        df_real["iata_id"] = df_real["icao_code"].map(icao_to_iata)

        df_real = df_real[
            (df_real["adep"] == "WARR") &
            (df_real["status_flight"] == "REGULER")
        ]

        if jenis == "domestik":
            df_real = df_real[df_real["ades"].str.startswith(("WA", "WI"), na=False)]
        elif jenis == "internasional":
            df_real = df_real[~df_real["ades"].str.startswith(("WA", "WI"), na=False)]

        df_real = df_real[df_real["tanggal"].between(start, end)]
        df_real = df_real[df_real["iata_id"].isin(all_iata_ids)]
    else:
        df_real = pd.DataFrame(columns=["tanggal", "iata_id", "ades"])

    # Planning
    cur.execute("""
        SELECT tanggal, iata_id, departure, arrival, departure_icao, arrival_icao 
        FROM main_prpp_generated
    """)
    df_plan = pd.DataFrame(cur.fetchall(), columns=[
        "tanggal", "iata_id", "departure", "arrival", "departure_icao", "arrival_icao"
    ])

    if not df_plan.empty:
        df_plan["tanggal"] = pd.to_datetime(df_plan["tanggal"])
        df_plan["iata_id"] = df_plan["iata_id"].fillna("").str.upper()
        df_plan["departure_icao"] = df_plan["departure_icao"].fillna("")
        df_plan["arrival_icao"] = df_plan["arrival_icao"].fillna("")

        if jenis == "domestik":
            mask_dom = (
                df_plan["departure_icao"].str.startswith(("WA", "WI"), na=False) &
                df_plan["arrival_icao"].str.startswith(("WA", "WI"), na=False)
            )
            df_plan = df_plan[mask_dom]
        elif jenis == "internasional":
            mask_int = ~(
                df_plan["departure_icao"].str.startswith(("WA", "WI"), na=False) &
                df_plan["arrival_icao"].str.startswith(("WA", "WI"), na=False)
            )
            df_plan = df_plan[mask_int]

        df_plan = df_plan[df_plan["tanggal"].between(start, end)]
        df_plan = df_plan[df_plan["iata_id"].isin(all_iata_ids)]
    else:
        df_plan = pd.DataFrame(columns=["tanggal", "iata_id", "arrival_icao"])

    cur.close()

    all_real = df_real.copy()
    all_plan = df_plan.copy()

    # Filter airline
    if selected_airline:
        iata_code = name_to_iata.get(selected_airline)
        if iata_code:
            df_real = df_real[df_real["iata_id"] == iata_code]
            df_plan = df_plan[df_plan["iata_id"] == iata_code]

    # Filter departure
    if selected_departure:
        df_plan = df_plan[df_plan["departure_icao"] == selected_departure]

    # Filter arrival
    if selected_arrival:
        df_real = df_real[df_real["ades"] == selected_arrival]
        df_plan = df_plan[df_plan["arrival_icao"] == selected_arrival]

    # Rekap
    plan_counts = df_plan["iata_id"].value_counts()
    real_counts = df_real["iata_id"].value_counts()
    maskapai_terlibat = sorted(set(plan_counts.index) | set(real_counts.index))

    rekap_data = []
    airline_names = sorted({iata_to_name[iata] for iata in all_iata_ids if iata in iata_to_name})

    for iata in maskapai_terlibat:
        plan = int(plan_counts.get(iata, 0))
        if plan == 0:
            continue
        real = int(real_counts.get(iata, 0))
        name = iata_to_name.get(iata, f"{iata} (unknown)")
        percent = (real / plan * 100) if plan > 0 else 0
        rekap_data.append((name, plan, real, round(percent, 2)))

    total_plan = sum(r[1] for r in rekap_data)
    total_real = sum(r[2] for r in rekap_data)

    # Line chart
    dates = pd.date_range(start, end)
    date_labels = [d.strftime("%Y-%m-%d") for d in dates]

    timeline = {
        "__TOTAL__": {"planning": [0] * len(dates), "realisasi": [0] * len(dates)},
        "__ALL_REAL__": {}
    }

    for iata in maskapai_terlibat:
        plan = int(plan_counts.get(iata, 0))
        if plan == 0:
            continue
        name = iata_to_name.get(iata, f"{iata} (unknown)")

        plan_range = df_plan[df_plan["iata_id"] == iata]
        real_range = df_real[df_real["iata_id"] == iata]

        plan_series = plan_range["tanggal"].value_counts().reindex(dates, fill_value=0)
        real_series = real_range["tanggal"].value_counts().reindex(dates, fill_value=0)

        timeline[name] = {
            "planning": plan_series.tolist(),
            "realisasi": real_series.tolist()
        }

        timeline["__TOTAL__"]["planning"] = [x + y for x, y in zip(timeline["__TOTAL__"]["planning"], plan_series)]
        timeline["__TOTAL__"]["realisasi"] = [x + y for x, y in zip(timeline["__TOTAL__"]["realisasi"], real_series)]
        timeline["__ALL_REAL__"][name] = real_series.tolist()

    # Dropdown values sesuai jenis
    all_departures = all_plan["departure_icao"].dropna().unique()
    all_arrivals = all_real["ades"].dropna().unique()

    if jenis == "domestik":
        all_departures = [code for code in all_departures if code.startswith(("WA", "WI"))]
        all_arrivals = [code for code in all_arrivals if code.startswith(("WA", "WI"))]

    unique_departures = sorted([
        (icao_to_nama_lokasi.get(code, code), code) for code in all_departures
    ])
    unique_arrivals = sorted([
        (icao_to_nama_lokasi.get(code, code), code) for code in all_arrivals
    ])

    return render_template(
        "flight_compare.html",
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        date_labels=json.dumps(date_labels),
        airline_names=airline_names,
        timeline=json.dumps(timeline),
        rekap_data=rekap_data,
        total_plan=total_plan,
        total_real=total_real,
        unique_departures=unique_departures,
        unique_arrivals=unique_arrivals,
        selected_airline=selected_airline or "",
        selected_route=selected_route or "",
        selected_departure=selected_departure or "",
        selected_arrival=selected_arrival or "",
        selected_jenis=jenis or "semua"
    )
@app.route("/flight-summary")
def flight_summary():
    # Ambil parameter tanggal dari URL
    start_str = request.args.get("start")
    end_str = request.args.get("end")

    # Jika tidak ada, default ke 30 hari terakhir
    if not start_str or not end_str:
        end = datetime.today().date()
        start = end - pd.Timedelta(days=30)
    else:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()

    cur = mysql.connection.cursor()

    # Load maskapai
    cur.execute("SELECT iata_code, icao_code, Maskapai FROM corpus_airline")
    airlines = cur.fetchall()
    icao_to_name = {r[1].upper(): r[2] for r in airlines}

    # Load data bandara (jika dibutuhkan untuk info lebih lanjut)
    cur.execute("SELECT ICAO_CODE, NAME_Airport, LOCATION FROM corpus_airport")
    airports = cur.fetchall()
    icao_to_airport = {
        r[0].upper(): f"{r[1]} ({r[2]})" for r in airports if r[1] and r[2]
    }

    # Ambil data realisasi
    cur.execute("SELECT tanggal, acid, adep, ades, status_flight FROM main_realisasi")
    rows = cur.fetchall()
    cur.close()

    df = pd.DataFrame(rows, columns=["tanggal", "acid", "adep", "ades", "status_flight"])
    df["tanggal"] = pd.to_datetime(df["tanggal"]).dt.date
    df = df[(df["tanggal"] >= start) & (df["tanggal"] <= end)]

    # Jika kosong, tampilkan halaman kosong
    if df.empty:
        return render_template("flight_summary.html", chart_data={}, table_data={}, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))

    # Proses maskapai
    df["icao_code"] = df["acid"].str[:3].str.upper()
    df["maskapai"] = df["icao_code"].apply(lambda x: icao_to_name.get(x, x))  # fallback ICAO jika tidak ada

    # Proses kolom tambahan
    df["status_flight"] = df["status_flight"].str.upper().fillna("UNKNOWN")
    df["rute"] = df["adep"].fillna("UNKNOWN").str.upper() + " → " + df["ades"].fillna("UNKNOWN").str.upper()
    df["jenis"] = df.apply(
        lambda row: "Domestik" if row["adep"].startswith(("WA", "WI")) and row["ades"].startswith(("WA", "WI")) else "Internasional",
        axis=1
    )
    df["arah"] = df.apply(lambda row: "Departure" if row["adep"] == "WARR" else "Arrival", axis=1)

    # Inisialisasi data
    chart_data = {}
    table_data = {}

    # Fungsi generate chart & tabel
    def generate_chart(groupby_col, label="label"):
        grp = df.groupby(groupby_col).size().reset_index(name="jumlah")
        grp[label] = grp[groupby_col]
        grp = grp.sort_values("jumlah", ascending=False).reset_index(drop=True)
        total = grp["jumlah"].sum()
        grp["persen"] = (grp["jumlah"] / total * 100).round(2)

        chart_data[groupby_col] = {
            "labels": grp[label].fillna("Unknown").tolist(),
            "jumlah": grp["jumlah"].tolist()
        }
        table_data[groupby_col] = grp[[label, "jumlah", "persen"]].rename(columns={label: "label"}).to_dict(orient="records")

    # Generate chart dan tabel untuk setiap kategori
    generate_chart("status_flight")
    generate_chart("maskapai")
    generate_chart("rute")
    generate_chart("jenis")
    generate_chart("arah")

    return render_template(
        "flight_summary.html",
        chart_data=chart_data,
        table_data=table_data,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d")
    )
import numpy as np
from flask import render_template

@app.route("/mcdm")
def mcdm():
    cur = mysql.connection.cursor()

    # ICAO → Nama Maskapai
    cur.execute("SELECT ICAO_CODE, Maskapai FROM corpus_airline")
    icao_to_name = dict(cur.fetchall())

    # Total delay > 15 menit
    cur.execute("""
        SELECT mp.airline_icao, SUM(hdp.delay_minutes)
        FROM hasil_delay_pushback hdp
        JOIN main_prpp_generated mp ON mp.id = hdp.id_prpp
        WHERE mp.departure_icao = 'WARR' AND hdp.delay_minutes > 15
        GROUP BY mp.airline_icao
    """)
    delay_dict = {}
    total_delay_all = 0
    for icao, total_delay in cur.fetchall():
        total_delay = float(total_delay or 0)
        delay_dict[icao] = total_delay
        total_delay_all += total_delay

    # Planning
    cur.execute("""
        SELECT airline_icao, COUNT(*) FROM main_prpp_generated
        WHERE departure_icao = 'WARR'
        GROUP BY airline_icao
    """)
    planning = {icao: count for icao, count in cur.fetchall()}

    # Realisasi
    cur.execute("""
        SELECT acid, COUNT(*) FROM main_realisasi
        WHERE adep = 'WARR' AND status_flight = 'REGULER'
        GROUP BY acid
    """)
    realisasi = {}
    for acid, count in cur.fetchall():
        if acid:
            icao = acid[:3].upper()
            realisasi[icao] = realisasi.get(icao, 0) + count

    # Gabungkan data
    data = []
    util_total = 0

    for icao, name in icao_to_name.items():
        delay = delay_dict.get(icao, 0.0)
        plan = planning.get(icao, 0)
        real = realisasi.get(icao, 0)
        util = real / plan if plan else 0
        util_total += util

        data.append({
            "icao": icao,
            "maskapai": name,
            "total_delay": delay,
            "plan": plan,
            "real": real,
            "utilitas": round(util, 3),
        })

    # Hitung ontime% dan utilitas%
    for d in data:
        delay = d["total_delay"]
        util = d["utilitas"]

        delay_percent = (delay / total_delay_all * 100) if total_delay_all else 0
        ontime_percent = 100-delay_percent  # Semakin kecil delay → semakin tinggi ontime
        util_percent = (util / util_total * 100) if util_total else 0

        d["ontime_percent"] = round(ontime_percent, 4)
        d["utilitas_percent"] = round(util_percent, 4)

    # Matriks TOPSIS
    matrix = np.array([[d["ontime_percent"], d["utilitas_percent"]] for d in data])

    # Normalisasi
    norm_matrix = matrix / np.sqrt((matrix ** 2).sum(axis=0))

    # Bobot
    weights = np.array([0.5, 0.5])
    weighted_matrix = norm_matrix * weights

    # Solusi ideal positif dan negatif
    ideal_plus = np.max(weighted_matrix, axis=0)
    ideal_minus = np.min(weighted_matrix, axis=0)

    # Jarak ke solusi
    dist_plus = np.linalg.norm(weighted_matrix - ideal_plus, axis=1)
    dist_minus = np.linalg.norm(weighted_matrix - ideal_minus, axis=1)

    # Skor preferensi TOPSIS
    scores = dist_minus / (dist_plus + dist_minus)

    # Tambahkan skor ke data
    for i, d in enumerate(data):
        d["final_score"] = round(float(scores[i]) * 100, 2)  # Persentase

    # Urutkan berdasarkan skor
    data = sorted(data, key=lambda x: x["final_score"], reverse=True)

    cur.close()
    return render_template("mcdm.html", result=data)

if __name__ == '__main__':
    app.run(debug=True)