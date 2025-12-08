import sqlite3
import os
from typing import List, Dict, Any

DATABASE_PATH = 'kargo_data.db'

def get_db_connection():
    """SQLite veritabanı bağlantısı oluştur"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def dict_from_row(row):
    """SQLite Row objesini dict'e çevir"""
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}

def hesapla_gercek_km(plaka, conn=None):
    """
    Bir aracın gerçek gidilen kilometresini hesapla

    DOĞRU HESAPLAMA (ARDIŞIK FARKLAR TOPLAMI):
    - Tarih sırasına göre sıralı kayıtlar arasındaki KM farklarını topla
    - Örnek: (km2-km1) + (km3-km2) + ... = Toplam gidilen yol
    - Km sayacı sıfırlanmış olsa bile doğru çalışır
    - Anomali kayıtları filtreler (negatif farkları atlar)

    Args:
        plaka: Araç plakası
        conn: Veritabanı bağlantısı (opsiyonel)

    Returns:
        float: Toplam gidilen kilometre (ardışık farklar toplamı)
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True

    try:
        cursor = conn.cursor()

        # Tüm km kayıtlarını TARİH SIRASINA göre al
        cursor.execute('''
            SELECT km_bilgisi
            FROM yakit
            WHERE plaka = ?
            AND km_bilgisi IS NOT NULL
            AND km_bilgisi > 0
            ORDER BY islem_tarihi ASC, id ASC
        ''', (plaka,))

        rows = cursor.fetchall()

        if len(rows) < 2:
            return 0

        # DEBUG: 46AJH283 için detaylı log
        if plaka == '46AJH283':
            print(f"\n{'='*60}")
            print(f"DEBUG: {plaka} için {len(rows)} kayıt bulundu")
            print(f"{'='*60}")
            for idx, row in enumerate(rows, 1):
                print(f"{idx}. KM: {float(row['km_bilgisi']):,.0f}")

        # Ardışık kayıtlar arasındaki farkları topla
        toplam_km = 0
        onceki_km = None

        for row in rows:
            km = float(row['km_bilgisi'])

            if onceki_km is not None:
                fark = km - onceki_km

                # Sadece pozitif farkları topla (km sayacı ileri gitmiş)
                if fark > 0:
                    toplam_km += fark

                    # DEBUG: Pozitif farkları göster
                    if plaka == '46AJH283':
                        print(f"  Fark: {onceki_km:,.0f} → {km:,.0f} = +{fark:,.0f} km")

            onceki_km = km

        # DEBUG: Sonuç
        if plaka == '46AJH283':
            print(f"\nTOPLAM ARDIŞIK FARKLAR: {toplam_km:,.0f} km")
            print(f"İlk KM: {float(rows[0]['km_bilgisi']):,.0f}")
            print(f"Son KM: {float(rows[-1]['km_bilgisi']):,.0f}")
            print(f"Basit Fark: {float(rows[-1]['km_bilgisi']) - float(rows[0]['km_bilgisi']):,.0f} km")
            print(f"{'='*60}\n")

        return toplam_km

    finally:
        if close_conn:
            conn.close()

def get_yakit_data():
    """Sadece aktif araçların yakıt verilerini çek"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        if araclar_exists:
            cursor.execute('''
                SELECT y.* FROM yakit y
                LEFT JOIN araclar a ON y.plaka = a.plaka
                WHERE a.plaka IS NULL OR a.aktif = 1
            ''')
        else:
            cursor.execute('SELECT * FROM yakit')

        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Yakıt verisi çekilemedi: {e}")
        return []

def get_agirlik_data():
    """Sadece aktif araçların ağırlık (kantar) verilerini çek"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        if araclar_exists:
            cursor.execute('''
                SELECT ag.* FROM agirlik ag
                LEFT JOIN araclar a ON ag.plaka = a.plaka
                WHERE a.plaka IS NULL OR a.aktif = 1
            ''')
        else:
            cursor.execute('SELECT * FROM agirlik')

        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Ağırlık verisi çekilemedi: {e}")
        return []

def get_arac_takip_data():
    """Araç takip verilerini çek"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM arac_takip')
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Araç takip verisi çekilemedi: {e}")
        return []

def get_all_plakas():
    """Aktif araçların plakalarını getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        all_plakalar = set()

        cursor.execute('SELECT DISTINCT plaka FROM yakit WHERE plaka IS NOT NULL')
        for row in cursor.fetchall():
            all_plakalar.add(row['plaka'])

        cursor.execute('SELECT DISTINCT plaka FROM agirlik WHERE plaka IS NOT NULL')
        for row in cursor.fetchall():
            all_plakalar.add(row['plaka'])

        cursor.execute('SELECT DISTINCT plaka FROM arac_takip WHERE plaka IS NOT NULL')
        for row in cursor.fetchall():
            all_plakalar.add(row['plaka'])

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        if araclar_exists:
            cursor.execute('SELECT plaka FROM araclar WHERE aktif = 1')
            aktif_plakalar = set([row['plaka'] for row in cursor.fetchall()])

            if aktif_plakalar:
                all_plakalar = all_plakalar.intersection(aktif_plakalar)

        conn.close()
        return sorted(list(all_plakalar))
    except Exception as e:
        print(f"Plakalar getirilemedi: {e}")
        return []

def get_yakit_by_plaka(plaka):
    """Belirli bir plakaya ait yakıt verilerini getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM yakit WHERE plaka = ?', (plaka,))
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Plaka bazlı yakıt verisi çekilemedi: {e}")
        return []

def get_agirlik_by_plaka(plaka, sadece_urun=False):
    """Belirli bir plakaya ait ağırlık verilerini getir

    Args:
        plaka: Araç plakası
        sadece_urun: True ise sadece ürün kayıtlarını getir (Adet hariç)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if sadece_urun:
            cursor.execute('''
                SELECT * FROM agirlik
                WHERE plaka = ?
                AND birim NOT IN ('Adet', 'adet', 'ADET')
            ''', (plaka,))
        else:
            cursor.execute('SELECT * FROM agirlik WHERE plaka = ?', (plaka,))

        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Plaka bazlı ağırlık verisi çekilemedi: {e}")
        return []

def get_arac_takip_by_plaka(plaka):
    """Belirli bir plakaya ait araç takip verilerini getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM arac_takip WHERE plaka = ?', (plaka,))
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Plaka bazlı araç takip verisi çekilemedi: {e}")
        return []

def get_statistics():
    """Genel istatistikleri hesapla"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) as count FROM yakit')
        yakit_count = cursor.fetchone()['count']

        cursor.execute('SELECT COUNT(*) as count FROM agirlik WHERE birim NOT IN ("Adet", "adet", "ADET")')
        agirlik_count = cursor.fetchone()['count']

        cursor.execute('SELECT COUNT(*) as count FROM arac_takip')
        arac_count = cursor.fetchone()['count']

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        toplam_yakit = 0.0
        toplam_maliyet = 0.0
        plaka_sayisi = 0
        plakalar = []

        if araclar_exists:
            cursor.execute('''
                SELECT y.yakit_miktari, y.satir_tutari
                FROM yakit y
                INNER JOIN araclar a ON y.plaka = a.plaka
                WHERE y.yakit_miktari IS NOT NULL
                AND y.yakit_miktari > 0
                AND a.aktif = 1
                AND a.arac_tipi = 'KARGO ARACI'
            ''')
            yakit_data = cursor.fetchall()

            for row in yakit_data:
                try:
                    yakit_val = row['yakit_miktari']
                    if yakit_val is not None and str(yakit_val).strip() != '':
                        toplam_yakit += float(yakit_val)
                except (ValueError, TypeError):
                    pass

                try:
                    tutar_val = row['satir_tutari']
                    if tutar_val is not None and str(tutar_val).strip() != '':
                        toplam_maliyet += float(tutar_val)
                except (ValueError, TypeError):
                    pass

            cursor.execute('''
                SELECT COUNT(DISTINCT plaka) as count
                FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'KARGO ARACI'
            ''')
            plaka_sayisi = cursor.fetchone()['count']

            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'KARGO ARACI'
                ORDER BY plaka
            ''')
            plakalar = [row['plaka'] for row in cursor.fetchall()]
        else:
            cursor.execute('''
                SELECT yakit_miktari, satir_tutari
                FROM yakit
                WHERE yakit_miktari IS NOT NULL AND yakit_miktari > 0
            ''')
            yakit_data = cursor.fetchall()

            for row in yakit_data:
                try:
                    yakit_val = row['yakit_miktari']
                    if yakit_val is not None and str(yakit_val).strip() != '':
                        toplam_yakit += float(yakit_val)
                except (ValueError, TypeError):
                    pass

                try:
                    tutar_val = row['satir_tutari']
                    if tutar_val is not None and str(tutar_val).strip() != '':
                        toplam_maliyet += float(tutar_val)
                except (ValueError, TypeError):
                    pass

            cursor.execute('SELECT COUNT(DISTINCT plaka) as count FROM yakit')
            plaka_sayisi = cursor.fetchone()['count']

            cursor.execute('SELECT DISTINCT plaka FROM yakit ORDER BY plaka')
            plakalar = [row['plaka'] for row in cursor.fetchall()]

        conn.close()

        return {
            'toplam_kayit': yakit_count + agirlik_count + arac_count,
            'yakit_kayit': yakit_count,
            'agirlik_kayit': agirlik_count,
            'arac_takip_kayit': arac_count,
            'plaka_sayisi': plaka_sayisi,
            'toplam_yakit': toplam_yakit,
            'toplam_maliyet': toplam_maliyet,
            'plakalar': plakalar
        }
    except Exception as e:
        print(f"İstatistikler hesaplanamadı: {e}")
        return {
            'toplam_kayit': 0,
            'yakit_kayit': 0,
            'agirlik_kayit': 0,
            'arac_takip_kayit': 0,
            'plaka_sayisi': 0,
            'toplam_yakit': 0,
            'toplam_maliyet': 0,
            'plakalar': []
        }

def check_database_exists():
    """Veritabanı dosyasının varlığını kontrol et"""
    return os.path.exists(DATABASE_PATH)

def get_all_araclar():
    """Tüm araçları getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM araclar ORDER BY plaka')
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Araçlar getirilemedi: {e}")
        return []

def add_arac(plaka, sahip, arac_tipi, notlar=''):
    """Yeni araç ekle"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO araclar (plaka, sahip, arac_tipi, notlar, aktif)
            VALUES (?, ?, ?, ?, 1)
        ''', (plaka, sahip, arac_tipi, notlar))
        conn.commit()
        conn.close()
        return {'status': 'success', 'message': 'Araç başarıyla eklendi'}
    except sqlite3.IntegrityError:
        return {'status': 'error', 'message': 'Bu plaka zaten kayıtlı!'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def update_arac(plaka, sahip, arac_tipi, aktif, notlar=''):
    """Araç bilgilerini güncelle"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE araclar
            SET sahip = ?, arac_tipi = ?, aktif = ?, notlar = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE plaka = ?
        ''', (sahip, arac_tipi, aktif, notlar, plaka))
        conn.commit()
        conn.close()
        return {'status': 'success', 'message': 'Araç güncellendi'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def delete_arac(plaka):
    """Araç sil"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM araclar WHERE plaka = ?', (plaka,))
        conn.commit()
        conn.close()
        return {'status': 'success', 'message': 'Araç silindi'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def bulk_import_araclar():
    """Tüm plakaları toplu olarak araclar tablosuna ekle - HIZLI VERSİYON"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR IGNORE INTO araclar (plaka, sahip, arac_tipi, notlar, aktif)
            SELECT DISTINCT plaka, 'BİZİM', 'KARGO ARACI', 'Otomatik eklendi', 1
            FROM yakit
            WHERE plaka IS NOT NULL AND plaka != ''
        ''')

        eklenen = cursor.rowcount

        cursor.execute('SELECT COUNT(*) FROM araclar')
        toplam = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        return {
            'status': 'success',
            'eklenen': eklenen,
            'toplam': toplam,
            'message': f'{eklenen} yeni plaka eklendi'
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def get_aktif_kargo_araclari(dahil_taseron=False):
    """Sadece aktif kargo araçlarını getir

    Args:
        dahil_taseron: True ise taşeron araçlar da dahil edilir
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if dahil_taseron:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'KARGO ARACI'
            ''')
        else:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1
                AND arac_tipi = 'KARGO ARACI'
                AND sahip = 'BİZİM'
            ''')

        rows = cursor.fetchall()
        conn.close()
        return [row['plaka'] for row in rows]
    except Exception as e:
        print(f"Aktif kargo araçları getirilemedi: {e}")
        return []

def get_aktif_binek_araclar(dahil_taseron=False):
    """Sadece aktif binek araçları getir

    Args:
        dahil_taseron: True ise taşeron araçlar da dahil edilir
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if dahil_taseron:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'BİNEK ARAÇ'
            ''')
        else:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1
                AND arac_tipi = 'BİNEK ARAÇ'
                AND sahip = 'BİZİM'
            ''')

        rows = cursor.fetchall()
        conn.close()
        return [row['plaka'] for row in rows]
    except Exception as e:
        print(f"Aktif binek araçları getirilemedi: {e}")
        return []

def get_aktif_is_makineleri(dahil_taseron=False):
    """Sadece aktif iş makinelerini getir

    Args:
        dahil_taseron: True ise taşeron araçlar da dahil edilir
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if dahil_taseron:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'İŞ MAKİNESİ'
            ''')
        else:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1
                AND arac_tipi = 'İŞ MAKİNESİ'
                AND sahip = 'BİZİM'
            ''')

        rows = cursor.fetchall()
        conn.close()
        return [row['plaka'] for row in rows]
    except Exception as e:
        print(f"Aktif iş makineleri getirilemedi: {e}")
        return []

def plaka_filtre_uygula():
    """Analizlerde kullanılacak plaka filtresini döndür

    Returns:
        tuple: (WHERE clause, parameters tuple)
    """
    try:
        aktif_plakalar = get_aktif_kargo_araclari()
        if not aktif_plakalar:
            return "", ()

        placeholders = ','.join('?' * len(aktif_plakalar))
        where_clause = f"plaka IN ({placeholders})"
        return where_clause, tuple(aktif_plakalar)
    except:
        return "", ()

def get_muhasebe_data(baslangic_tarihi, bitis_tarihi, plaka=None):
    """Muhasebe verilerini hesapla"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Tarih filtresi oluştur
        if baslangic_tarihi and bitis_tarihi:
            tarih_filtre_yakit = "WHERE islem_tarihi BETWEEN ? AND ?"
            tarih_filtre_agirlik = "WHERE tarih BETWEEN ? AND ?"
            tarih_params = (baslangic_tarihi, bitis_tarihi)
        else:
            tarih_filtre_yakit = ""
            tarih_filtre_agirlik = ""
            tarih_params = ()

        # Plaka filtresi ekle - SADECE AKTİF KARGO ARAÇLARI
        if plaka:
            yakit_query = f'''
                SELECT y.plaka, SUM(y.satir_tutari) as toplam_gider
                FROM yakit y
                INNER JOIN araclar a ON y.plaka = a.plaka
                {tarih_filtre_yakit.replace('islem_tarihi', 'y.islem_tarihi')}
                {"AND" if tarih_filtre_yakit else "WHERE"} y.plaka = ?
                AND a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY y.plaka
            '''
            agirlik_query = f'''
                SELECT ag.plaka, SUM(ag.net_agirlik * 0.5) as toplam_gelir, MAX(ag.ana_malzeme) as ana_malzeme
                FROM agirlik ag
                INNER JOIN araclar a ON ag.plaka = a.plaka
                {tarih_filtre_agirlik.replace('tarih', 'ag.tarih')}
                {"AND" if tarih_filtre_agirlik else "WHERE"} ag.plaka = ?
                AND ag.birim NOT IN ('Adet', 'adet', 'ADET')
                AND a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY ag.plaka
            '''
            cursor.execute(yakit_query, tarih_params + (plaka,))
            yakit_rows = cursor.fetchall()
            cursor.execute(agirlik_query, tarih_params + (plaka,))
            agirlik_rows = cursor.fetchall()
        else:
            yakit_query = f'''
                SELECT y.plaka, SUM(y.satir_tutari) as toplam_gider
                FROM yakit y
                INNER JOIN araclar a ON y.plaka = a.plaka
                {tarih_filtre_yakit.replace('islem_tarihi', 'y.islem_tarihi')}
                {"WHERE" if not tarih_filtre_yakit else "AND"} a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY y.plaka
            '''
            agirlik_query = f'''
                SELECT ag.plaka, SUM(ag.net_agirlik * 0.5) as toplam_gelir, MAX(ag.ana_malzeme) as ana_malzeme
                FROM agirlik ag
                INNER JOIN araclar a ON ag.plaka = a.plaka
                {tarih_filtre_agirlik.replace('tarih', 'ag.tarih')}
                {"WHERE" if not tarih_filtre_agirlik else "AND"} ag.birim NOT IN ('Adet', 'adet', 'ADET')
                AND a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY ag.plaka
            '''
            cursor.execute(yakit_query, tarih_params)
            yakit_rows = cursor.fetchall()
            cursor.execute(agirlik_query, tarih_params)
            agirlik_rows = cursor.fetchall()

        conn.close()

        plaka_veriler = {}
        for row in yakit_rows:
            p = row['plaka']
            if p not in plaka_veriler:
                plaka_veriler[p] = {'gelir': 0, 'gider': 0, 'ana_malzeme': 'Bilinmiyor'}
            plaka_veriler[p]['gider'] = float(row['toplam_gider'] or 0)

        for row in agirlik_rows:
            p = row['plaka']
            if p not in plaka_veriler:
                plaka_veriler[p] = {'gelir': 0, 'gider': 0, 'ana_malzeme': 'Bilinmiyor'}
            plaka_veriler[p]['gelir'] = float(row['toplam_gelir'] or 0)
            plaka_veriler[p]['ana_malzeme'] = row['ana_malzeme'] or 'Bilinmiyor'

        toplam_gelir = sum(v['gelir'] for v in plaka_veriler.values())
        toplam_gider = sum(v['gider'] for v in plaka_veriler.values())
        net_kar = toplam_gelir - toplam_gider
        kar_marji = (net_kar / toplam_gelir * 100) if toplam_gelir > 0 else 0

        plaka_bazli = []
        for p, v in plaka_veriler.items():
            net = v['gelir'] - v['gider']
            marji = (net / v['gelir'] * 100) if v['gelir'] > 0 else 0
            plaka_bazli.append({
                'plaka': p,
                'gelir': v['gelir'],
                'gider': v['gider'],
                'net_kar': net,
                'kar_marji': marji,
                'ana_malzeme': v['ana_malzeme']
            })

        plaka_bazli.sort(key=lambda x: x['net_kar'], reverse=True)

        return {
            'status': 'success',
            'toplam_gelir': toplam_gelir,
            'toplam_gider': toplam_gider,
            'net_kar': net_kar,
            'kar_marji': kar_marji,
            'plaka_bazli': plaka_bazli
        }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }

def get_arac_performans_analizi(plaka, baslangic_tarihi=None, bitis_tarihi=None):
    """Araç performans analizi - yakıt/km oranı ve tonaj bilgisi"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Tarih filtresi
        if baslangic_tarihi and bitis_tarihi:
            tarih_filtre_yakit = "AND islem_tarihi BETWEEN ? AND ?"
            tarih_filtre_agirlik = "AND tarih BETWEEN ? AND ?"
            tarih_params = (baslangic_tarihi, bitis_tarihi)
        else:
            tarih_filtre_yakit = ""
            tarih_filtre_agirlik = ""
            tarih_params = ()

        # Yakıt ve KM bilgisi
        yakit_query = f'''
            SELECT
                SUM(yakit_miktari) as toplam_yakit,
                SUM(km_bilgisi) as toplam_km,
                COUNT(*) as sefer_sayisi,
                AVG(yakit_miktari) as ort_yakit_sefer,
                AVG(birim_fiyat) as ort_birim_fiyat,
                SUM(satir_tutari) as toplam_maliyet
            FROM yakit
            WHERE plaka = ? {tarih_filtre_yakit}
            AND yakit_miktari IS NOT NULL AND yakit_miktari > 0
        '''
        cursor.execute(yakit_query, (plaka,) + tarih_params)
        yakit_row = cursor.fetchone()

        # Tonaj bilgisi (ağırlık tablosundan) - SADECE ÜRÜN (Adet HARİÇ)
        agirlik_query = f'''
            SELECT
                SUM(net_agirlik) as toplam_tonaj,
                COUNT(*) as yuklenme_sayisi,
                AVG(net_agirlik) as ort_tonaj_yuklenme
            FROM agirlik
            WHERE plaka = ? {tarih_filtre_agirlik}
            AND net_agirlik IS NOT NULL AND net_agirlik > 0
            AND birim NOT IN ('Adet', 'adet', 'ADET')
        '''
        cursor.execute(agirlik_query, (plaka,) + tarih_params)
        agirlik_row = cursor.fetchone()

        conn.close()

        # Hesaplamalar
        toplam_yakit = float(yakit_row['toplam_yakit'] or 0)
        toplam_km = float(yakit_row['toplam_km'] or 0)
        sefer_sayisi = int(yakit_row['sefer_sayisi'] or 0)
        toplam_maliyet = float(yakit_row['toplam_maliyet'] or 0)
        ort_yakit_sefer = float(yakit_row['ort_yakit_sefer'] or 0)
        ort_birim_fiyat = float(yakit_row['ort_birim_fiyat'] or 0)

        toplam_tonaj = float(agirlik_row['toplam_tonaj'] or 0)
        yuklenme_sayisi = int(agirlik_row['yuklenme_sayisi'] or 0)
        ort_tonaj_yuklenme = float(agirlik_row['ort_tonaj_yuklenme'] or 0)

        # Yakıt/KM oranı
        yakit_km_orani = (toplam_yakit / toplam_km) if toplam_km > 0 else 0

        # KM başına maliyet
        km_basina_maliyet = (toplam_maliyet / toplam_km) if toplam_km > 0 else 0

        # Ton/Yakıt oranı (litre başına kaç ton taşındı - yüksek = verimli)
        toplam_tonaj_ton = toplam_tonaj / 1000  # kg'den ton'a çevir
        ton_basina_yakit = (toplam_tonaj_ton / toplam_yakit) if toplam_yakit > 0 else 0

        # Verimlilik skoru (düşük = iyi)
        verimlilik_skoru = yakit_km_orani * 100 if yakit_km_orani > 0 else 0

        return {
            'status': 'success',
            'plaka': plaka,
            'baslangic_tarihi': baslangic_tarihi or 'Başlangıç',
            'bitis_tarihi': bitis_tarihi or 'Bugün',
            'yakit': {
                'toplam_yakit': round(toplam_yakit, 2),
                'toplam_km': round(toplam_km, 2),
                'sefer_sayisi': sefer_sayisi,
                'ort_yakit_sefer': round(ort_yakit_sefer, 2),
                'ort_birim_fiyat': round(ort_birim_fiyat, 2),
                'toplam_maliyet': round(toplam_maliyet, 2)
            },
            'tonaj': {
                'toplam_tonaj': round(toplam_tonaj, 2),
                'yuklenme_sayisi': yuklenme_sayisi,
                'ort_tonaj_yuklenme': round(ort_tonaj_yuklenme, 2)
            },
            'performans': {
                'yakit_km_orani': round(yakit_km_orani, 3),
                'km_basina_maliyet': round(km_basina_maliyet, 2),
                'ton_basina_yakit': round(ton_basina_yakit, 2),
                'verimlilik_skoru': round(verimlilik_skoru, 2)
            }
        }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }

def get_database_info():
    """Veritabanı hakkında bilgi al"""
    if not check_database_exists():
        return {
            'exists': False,
            'path': DATABASE_PATH,
            'message': 'Veritabanı dosyası bulunamadı. Lütfen önce Excel dosyalarını yükleyin.'
        }

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row['name'] for row in cursor.fetchall()]

        table_info = {}
        for table in tables:
            cursor.execute(f'SELECT COUNT(*) as count FROM {table}')
            count = cursor.fetchone()['count']
            table_info[table] = count

        conn.close()

        return {
            'exists': True,
            'path': DATABASE_PATH,
            'tables': table_info,
            'message': 'Veritabanı bağlantısı başarılı'
        }
    except Exception as e:
        return {
            'exists': False,
            'path': DATABASE_PATH,
            'error': str(e),
            'message': f'Veritabanı hatası: {str(e)}'
        }
import sqlite3
import os
from typing import List, Dict, Any

DATABASE_PATH = 'kargo_data.db'

def get_db_connection():
    """SQLite veritabanı bağlantısı oluştur"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def dict_from_row(row):
    """SQLite Row objesini dict'e çevir"""
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}

def hesapla_gercek_km(plaka, conn=None):
    """
    Bir aracın gerçek gidilen kilometresini hesapla

    DOĞRU HESAPLAMA (ARDIŞIK FARKLAR TOPLAMI):
    - Tarih sırasına göre sıralı kayıtlar arasındaki KM farklarını topla
    - Örnek: (km2-km1) + (km3-km2) + ... = Toplam gidilen yol
    - Km sayacı sıfırlanmış olsa bile doğru çalışır
    - Anomali kayıtları filtreler (negatif farkları atlar)

    Args:
        plaka: Araç plakası
        conn: Veritabanı bağlantısı (opsiyonel)

    Returns:
        float: Toplam gidilen kilometre (ardışık farklar toplamı)
    """
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True

    try:
        cursor = conn.cursor()

        # Tüm km kayıtlarını TARİH SIRASINA göre al
        cursor.execute('''
            SELECT km_bilgisi
            FROM yakit
            WHERE plaka = ?
            AND km_bilgisi IS NOT NULL
            AND km_bilgisi > 0
            ORDER BY islem_tarihi ASC, id ASC
        ''', (plaka,))

        rows = cursor.fetchall()

        if len(rows) < 2:
            return 0

        # DEBUG: 46AJH283 için detaylı log
        if plaka == '46AJH283':
            print(f"\n{'='*60}")
            print(f"DEBUG: {plaka} için {len(rows)} kayıt bulundu")
            print(f"{'='*60}")
            for idx, row in enumerate(rows, 1):
                print(f"{idx}. KM: {float(row['km_bilgisi']):,.0f}")

        # Ardışık kayıtlar arasındaki farkları topla
        toplam_km = 0
        onceki_km = None

        for row in rows:
            km = float(row['km_bilgisi'])

            if onceki_km is not None:
                fark = km - onceki_km

                # Sadece pozitif farkları topla (km sayacı ileri gitmiş)
                if fark > 0:
                    toplam_km += fark

                    # DEBUG: Pozitif farkları göster
                    if plaka == '46AJH283':
                        print(f"  Fark: {onceki_km:,.0f} → {km:,.0f} = +{fark:,.0f} km")

            onceki_km = km

        # DEBUG: Sonuç
        if plaka == '46AJH283':
            print(f"\nTOPLAM ARDIŞIK FARKLAR: {toplam_km:,.0f} km")
            print(f"İlk KM: {float(rows[0]['km_bilgisi']):,.0f}")
            print(f"Son KM: {float(rows[-1]['km_bilgisi']):,.0f}")
            print(f"Basit Fark: {float(rows[-1]['km_bilgisi']) - float(rows[0]['km_bilgisi']):,.0f} km")
            print(f"{'='*60}\n")

        return toplam_km

    finally:
        if close_conn:
            conn.close()

def get_yakit_data():
    """Sadece aktif araçların yakıt verilerini çek"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        if araclar_exists:
            cursor.execute('''
                SELECT y.* FROM yakit y
                LEFT JOIN araclar a ON y.plaka = a.plaka
                WHERE a.plaka IS NULL OR a.aktif = 1
            ''')
        else:
            cursor.execute('SELECT * FROM yakit')

        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Yakıt verisi çekilemedi: {e}")
        return []

def get_agirlik_data():
    """Sadece aktif araçların ağırlık (kantar) verilerini çek"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        if araclar_exists:
            cursor.execute('''
                SELECT ag.* FROM agirlik ag
                LEFT JOIN araclar a ON ag.plaka = a.plaka
                WHERE a.plaka IS NULL OR a.aktif = 1
            ''')
        else:
            cursor.execute('SELECT * FROM agirlik')

        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Ağırlık verisi çekilemedi: {e}")
        return []

def get_arac_takip_data():
    """Araç takip verilerini çek"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM arac_takip')
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Araç takip verisi çekilemedi: {e}")
        return []

def get_all_plakas():
    """Aktif araçların plakalarını getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        all_plakalar = set()

        cursor.execute('SELECT DISTINCT plaka FROM yakit WHERE plaka IS NOT NULL')
        for row in cursor.fetchall():
            all_plakalar.add(row['plaka'])

        cursor.execute('SELECT DISTINCT plaka FROM agirlik WHERE plaka IS NOT NULL')
        for row in cursor.fetchall():
            all_plakalar.add(row['plaka'])

        cursor.execute('SELECT DISTINCT plaka FROM arac_takip WHERE plaka IS NOT NULL')
        for row in cursor.fetchall():
            all_plakalar.add(row['plaka'])

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        if araclar_exists:
            cursor.execute('SELECT plaka FROM araclar WHERE aktif = 1')
            aktif_plakalar = set([row['plaka'] for row in cursor.fetchall()])

            if aktif_plakalar:
                all_plakalar = all_plakalar.intersection(aktif_plakalar)

        conn.close()
        return sorted(list(all_plakalar))
    except Exception as e:
        print(f"Plakalar getirilemedi: {e}")
        return []

def get_yakit_by_plaka(plaka):
    """Belirli bir plakaya ait yakıt verilerini getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM yakit WHERE plaka = ?', (plaka,))
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Plaka bazlı yakıt verisi çekilemedi: {e}")
        return []

def get_agirlik_by_plaka(plaka, sadece_urun=False):
    """Belirli bir plakaya ait ağırlık verilerini getir

    Args:
        plaka: Araç plakası
        sadece_urun: True ise sadece ürün kayıtlarını getir (Adet hariç)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if sadece_urun:
            cursor.execute('''
                SELECT * FROM agirlik
                WHERE plaka = ?
                AND birim NOT IN ('Adet', 'adet', 'ADET')
            ''', (plaka,))
        else:
            cursor.execute('SELECT * FROM agirlik WHERE plaka = ?', (plaka,))

        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Plaka bazlı ağırlık verisi çekilemedi: {e}")
        return []

def get_arac_takip_by_plaka(plaka):
    """Belirli bir plakaya ait araç takip verilerini getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM arac_takip WHERE plaka = ?', (plaka,))
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Plaka bazlı araç takip verisi çekilemedi: {e}")
        return []

def get_statistics():
    """Genel istatistikleri hesapla"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) as count FROM yakit')
        yakit_count = cursor.fetchone()['count']

        cursor.execute('SELECT COUNT(*) as count FROM agirlik WHERE birim NOT IN ("Adet", "adet", "ADET")')
        agirlik_count = cursor.fetchone()['count']

        cursor.execute('SELECT COUNT(*) as count FROM arac_takip')
        arac_count = cursor.fetchone()['count']

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='araclar'")
        araclar_exists = cursor.fetchone() is not None

        toplam_yakit = 0.0
        toplam_maliyet = 0.0
        plaka_sayisi = 0
        plakalar = []

        if araclar_exists:
            cursor.execute('''
                SELECT y.yakit_miktari, y.satir_tutari
                FROM yakit y
                INNER JOIN araclar a ON y.plaka = a.plaka
                WHERE y.yakit_miktari IS NOT NULL
                AND y.yakit_miktari > 0
                AND a.aktif = 1
                AND a.arac_tipi = 'KARGO ARACI'
            ''')
            yakit_data = cursor.fetchall()

            for row in yakit_data:
                try:
                    yakit_val = row['yakit_miktari']
                    if yakit_val is not None and str(yakit_val).strip() != '':
                        toplam_yakit += float(yakit_val)
                except (ValueError, TypeError):
                    pass

                try:
                    tutar_val = row['satir_tutari']
                    if tutar_val is not None and str(tutar_val).strip() != '':
                        toplam_maliyet += float(tutar_val)
                except (ValueError, TypeError):
                    pass

            cursor.execute('''
                SELECT COUNT(DISTINCT plaka) as count
                FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'KARGO ARACI'
            ''')
            plaka_sayisi = cursor.fetchone()['count']

            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'KARGO ARACI'
                ORDER BY plaka
            ''')
            plakalar = [row['plaka'] for row in cursor.fetchall()]
        else:
            cursor.execute('''
                SELECT yakit_miktari, satir_tutari
                FROM yakit
                WHERE yakit_miktari IS NOT NULL AND yakit_miktari > 0
            ''')
            yakit_data = cursor.fetchall()

            for row in yakit_data:
                try:
                    yakit_val = row['yakit_miktari']
                    if yakit_val is not None and str(yakit_val).strip() != '':
                        toplam_yakit += float(yakit_val)
                except (ValueError, TypeError):
                    pass

                try:
                    tutar_val = row['satir_tutari']
                    if tutar_val is not None and str(tutar_val).strip() != '':
                        toplam_maliyet += float(tutar_val)
                except (ValueError, TypeError):
                    pass

            cursor.execute('SELECT COUNT(DISTINCT plaka) as count FROM yakit')
            plaka_sayisi = cursor.fetchone()['count']

            cursor.execute('SELECT DISTINCT plaka FROM yakit ORDER BY plaka')
            plakalar = [row['plaka'] for row in cursor.fetchall()]

        conn.close()

        return {
            'toplam_kayit': yakit_count + agirlik_count + arac_count,
            'yakit_kayit': yakit_count,
            'agirlik_kayit': agirlik_count,
            'arac_takip_kayit': arac_count,
            'plaka_sayisi': plaka_sayisi,
            'toplam_yakit': toplam_yakit,
            'toplam_maliyet': toplam_maliyet,
            'plakalar': plakalar
        }
    except Exception as e:
        print(f"İstatistikler hesaplanamadı: {e}")
        return {
            'toplam_kayit': 0,
            'yakit_kayit': 0,
            'agirlik_kayit': 0,
            'arac_takip_kayit': 0,
            'plaka_sayisi': 0,
            'toplam_yakit': 0,
            'toplam_maliyet': 0,
            'plakalar': []
        }

def check_database_exists():
    """Veritabanı dosyasının varlığını kontrol et"""
    return os.path.exists(DATABASE_PATH)

def get_all_araclar():
    """Tüm araçları getir"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM araclar ORDER BY plaka')
        rows = cursor.fetchall()
        conn.close()
        return [dict_from_row(row) for row in rows]
    except Exception as e:
        print(f"Araçlar getirilemedi: {e}")
        return []

def add_arac(plaka, sahip, arac_tipi, notlar=''):
    """Yeni araç ekle"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO araclar (plaka, sahip, arac_tipi, notlar, aktif)
            VALUES (?, ?, ?, ?, 1)
        ''', (plaka, sahip, arac_tipi, notlar))
        conn.commit()
        conn.close()
        return {'status': 'success', 'message': 'Araç başarıyla eklendi'}
    except sqlite3.IntegrityError:
        return {'status': 'error', 'message': 'Bu plaka zaten kayıtlı!'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def update_arac(plaka, sahip, arac_tipi, aktif, notlar=''):
    """Araç bilgilerini güncelle"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE araclar
            SET sahip = ?, arac_tipi = ?, aktif = ?, notlar = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE plaka = ?
        ''', (sahip, arac_tipi, aktif, notlar, plaka))
        conn.commit()
        conn.close()
        return {'status': 'success', 'message': 'Araç güncellendi'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def delete_arac(plaka):
    """Araç sil"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM araclar WHERE plaka = ?', (plaka,))
        conn.commit()
        conn.close()
        return {'status': 'success', 'message': 'Araç silindi'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def bulk_import_araclar():
    """Tüm plakaları toplu olarak araclar tablosuna ekle - HIZLI VERSİYON"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR IGNORE INTO araclar (plaka, sahip, arac_tipi, notlar, aktif)
            SELECT DISTINCT plaka, 'BİZİM', 'KARGO ARACI', 'Otomatik eklendi', 1
            FROM yakit
            WHERE plaka IS NOT NULL AND plaka != ''
        ''')

        eklenen = cursor.rowcount

        cursor.execute('SELECT COUNT(*) FROM araclar')
        toplam = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        return {
            'status': 'success',
            'eklenen': eklenen,
            'toplam': toplam,
            'message': f'{eklenen} yeni plaka eklendi'
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def get_aktif_kargo_araclari(dahil_taseron=False):
    """Sadece aktif kargo araçlarını getir

    Args:
        dahil_taseron: True ise taşeron araçlar da dahil edilir
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if dahil_taseron:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'KARGO ARACI'
            ''')
        else:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1
                AND arac_tipi = 'KARGO ARACI'
                AND sahip = 'BİZİM'
            ''')

        rows = cursor.fetchall()
        conn.close()
        return [row['plaka'] for row in rows]
    except Exception as e:
        print(f"Aktif kargo araçları getirilemedi: {e}")
        return []

def get_aktif_binek_araclar(dahil_taseron=False):
    """Sadece aktif binek araçları getir

    Args:
        dahil_taseron: True ise taşeron araçlar da dahil edilir
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if dahil_taseron:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'BİNEK ARAÇ'
            ''')
        else:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1
                AND arac_tipi = 'BİNEK ARAÇ'
                AND sahip = 'BİZİM'
            ''')

        rows = cursor.fetchall()
        conn.close()
        return [row['plaka'] for row in rows]
    except Exception as e:
        print(f"Aktif binek araçları getirilemedi: {e}")
        return []

def get_aktif_is_makineleri(dahil_taseron=False):
    """Sadece aktif iş makinelerini getir

    Args:
        dahil_taseron: True ise taşeron araçlar da dahil edilir
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if dahil_taseron:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1 AND arac_tipi = 'İŞ MAKİNESİ'
            ''')
        else:
            cursor.execute('''
                SELECT plaka FROM araclar
                WHERE aktif = 1
                AND arac_tipi = 'İŞ MAKİNESİ'
                AND sahip = 'BİZİM'
            ''')

        rows = cursor.fetchall()
        conn.close()
        return [row['plaka'] for row in rows]
    except Exception as e:
        print(f"Aktif iş makineleri getirilemedi: {e}")
        return []

def plaka_filtre_uygula():
    """Analizlerde kullanılacak plaka filtresini döndür

    Returns:
        tuple: (WHERE clause, parameters tuple)
    """
    try:
        aktif_plakalar = get_aktif_kargo_araclari()
        if not aktif_plakalar:
            return "", ()

        placeholders = ','.join('?' * len(aktif_plakalar))
        where_clause = f"plaka IN ({placeholders})"
        return where_clause, tuple(aktif_plakalar)
    except:
        return "", ()

def get_muhasebe_data(baslangic_tarihi, bitis_tarihi, plaka=None):
    """Muhasebe verilerini hesapla"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Tarih filtresi oluştur
        if baslangic_tarihi and bitis_tarihi:
            tarih_filtre_yakit = "WHERE islem_tarihi BETWEEN ? AND ?"
            tarih_filtre_agirlik = "WHERE tarih BETWEEN ? AND ?"
            tarih_params = (baslangic_tarihi, bitis_tarihi)
        else:
            tarih_filtre_yakit = ""
            tarih_filtre_agirlik = ""
            tarih_params = ()

        # Plaka filtresi ekle - SADECE AKTİF KARGO ARAÇLARI
        if plaka:
            yakit_query = f'''
                SELECT y.plaka, SUM(y.satir_tutari) as toplam_gider
                FROM yakit y
                INNER JOIN araclar a ON y.plaka = a.plaka
                {tarih_filtre_yakit.replace('islem_tarihi', 'y.islem_tarihi')}
                {"AND" if tarih_filtre_yakit else "WHERE"} y.plaka = ?
                AND a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY y.plaka
            '''
            agirlik_query = f'''
                SELECT ag.plaka, SUM(ag.net_agirlik * 0.5) as toplam_gelir, MAX(ag.ana_malzeme) as ana_malzeme
                FROM agirlik ag
                INNER JOIN araclar a ON ag.plaka = a.plaka
                {tarih_filtre_agirlik.replace('tarih', 'ag.tarih')}
                {"AND" if tarih_filtre_agirlik else "WHERE"} ag.plaka = ?
                AND ag.birim NOT IN ('Adet', 'adet', 'ADET')
                AND a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY ag.plaka
            '''
            cursor.execute(yakit_query, tarih_params + (plaka,))
            yakit_rows = cursor.fetchall()
            cursor.execute(agirlik_query, tarih_params + (plaka,))
            agirlik_rows = cursor.fetchall()
        else:
            yakit_query = f'''
                SELECT y.plaka, SUM(y.satir_tutari) as toplam_gider
                FROM yakit y
                INNER JOIN araclar a ON y.plaka = a.plaka
                {tarih_filtre_yakit.replace('islem_tarihi', 'y.islem_tarihi')}
                {"WHERE" if not tarih_filtre_yakit else "AND"} a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY y.plaka
            '''
            agirlik_query = f'''
                SELECT ag.plaka, SUM(ag.net_agirlik * 0.5) as toplam_gelir, MAX(ag.ana_malzeme) as ana_malzeme
                FROM agirlik ag
                INNER JOIN araclar a ON ag.plaka = a.plaka
                {tarih_filtre_agirlik.replace('tarih', 'ag.tarih')}
                {"WHERE" if not tarih_filtre_agirlik else "AND"} ag.birim NOT IN ('Adet', 'adet', 'ADET')
                AND a.aktif = 1 AND a.arac_tipi = 'KARGO ARACI'
                GROUP BY ag.plaka
            '''
            cursor.execute(yakit_query, tarih_params)
            yakit_rows = cursor.fetchall()
            cursor.execute(agirlik_query, tarih_params)
            agirlik_rows = cursor.fetchall()

        conn.close()

        plaka_veriler = {}
        for row in yakit_rows:
            p = row['plaka']
            if p not in plaka_veriler:
                plaka_veriler[p] = {'gelir': 0, 'gider': 0, 'ana_malzeme': 'Bilinmiyor'}
            plaka_veriler[p]['gider'] = float(row['toplam_gider'] or 0)

        for row in agirlik_rows:
            p = row['plaka']
            if p not in plaka_veriler:
                plaka_veriler[p] = {'gelir': 0, 'gider': 0, 'ana_malzeme': 'Bilinmiyor'}
            plaka_veriler[p]['gelir'] = float(row['toplam_gelir'] or 0)
            plaka_veriler[p]['ana_malzeme'] = row['ana_malzeme'] or 'Bilinmiyor'

        toplam_gelir = sum(v['gelir'] for v in plaka_veriler.values())
        toplam_gider = sum(v['gider'] for v in plaka_veriler.values())
        net_kar = toplam_gelir - toplam_gider
        kar_marji = (net_kar / toplam_gelir * 100) if toplam_gelir > 0 else 0

        plaka_bazli = []
        for p, v in plaka_veriler.items():
            net = v['gelir'] - v['gider']
            marji = (net / v['gelir'] * 100) if v['gelir'] > 0 else 0
            plaka_bazli.append({
                'plaka': p,
                'gelir': v['gelir'],
                'gider': v['gider'],
                'net_kar': net,
                'kar_marji': marji,
                'ana_malzeme': v['ana_malzeme']
            })

        plaka_bazli.sort(key=lambda x: x['net_kar'], reverse=True)

        return {
            'status': 'success',
            'toplam_gelir': toplam_gelir,
            'toplam_gider': toplam_gider,
            'net_kar': net_kar,
            'kar_marji': kar_marji,
            'plaka_bazli': plaka_bazli
        }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }

def get_arac_performans_analizi(plaka, baslangic_tarihi=None, bitis_tarihi=None):
    """Araç performans analizi - yakıt/km oranı ve tonaj bilgisi"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Tarih filtresi
        if baslangic_tarihi and bitis_tarihi:
            tarih_filtre_yakit = "AND islem_tarihi BETWEEN ? AND ?"
            tarih_filtre_agirlik = "AND tarih BETWEEN ? AND ?"
            tarih_params = (baslangic_tarihi, bitis_tarihi)
        else:
            tarih_filtre_yakit = ""
            tarih_filtre_agirlik = ""
            tarih_params = ()

        # Yakıt ve KM bilgisi
        yakit_query = f'''
            SELECT
                SUM(yakit_miktari) as toplam_yakit,
                SUM(km_bilgisi) as toplam_km,
                COUNT(*) as sefer_sayisi,
                AVG(yakit_miktari) as ort_yakit_sefer,
                AVG(birim_fiyat) as ort_birim_fiyat,
                SUM(satir_tutari) as toplam_maliyet
            FROM yakit
            WHERE plaka = ? {tarih_filtre_yakit}
            AND yakit_miktari IS NOT NULL AND yakit_miktari > 0
        '''
        cursor.execute(yakit_query, (plaka,) + tarih_params)
        yakit_row = cursor.fetchone()

        # Tonaj bilgisi (ağırlık tablosundan) - SADECE ÜRÜN (Adet HARİÇ)
        agirlik_query = f'''
            SELECT
                SUM(net_agirlik) as toplam_tonaj,
                COUNT(*) as yuklenme_sayisi,
                AVG(net_agirlik) as ort_tonaj_yuklenme
            FROM agirlik
            WHERE plaka = ? {tarih_filtre_agirlik}
            AND net_agirlik IS NOT NULL AND net_agirlik > 0
            AND birim NOT IN ('Adet', 'adet', 'ADET')
        '''
        cursor.execute(agirlik_query, (plaka,) + tarih_params)
        agirlik_row = cursor.fetchone()

        conn.close()

        # Hesaplamalar
        toplam_yakit = float(yakit_row['toplam_yakit'] or 0)
        toplam_km = float(yakit_row['toplam_km'] or 0)
        sefer_sayisi = int(yakit_row['sefer_sayisi'] or 0)
        toplam_maliyet = float(yakit_row['toplam_maliyet'] or 0)
        ort_yakit_sefer = float(yakit_row['ort_yakit_sefer'] or 0)
        ort_birim_fiyat = float(yakit_row['ort_birim_fiyat'] or 0)

        toplam_tonaj = float(agirlik_row['toplam_tonaj'] or 0)
        yuklenme_sayisi = int(agirlik_row['yuklenme_sayisi'] or 0)
        ort_tonaj_yuklenme = float(agirlik_row['ort_tonaj_yuklenme'] or 0)

        # Yakıt/KM oranı
        yakit_km_orani = (toplam_yakit / toplam_km) if toplam_km > 0 else 0

        # KM başına maliyet
        km_basina_maliyet = (toplam_maliyet / toplam_km) if toplam_km > 0 else 0

        # Ton/Yakıt oranı (litre başına kaç ton taşındı - yüksek = verimli)
        toplam_tonaj_ton = toplam_tonaj / 1000  # kg'den ton'a çevir
        ton_basina_yakit = (toplam_tonaj_ton / toplam_yakit) if toplam_yakit > 0 else 0

        # Verimlilik skoru (düşük = iyi)
        verimlilik_skoru = yakit_km_orani * 100 if yakit_km_orani > 0 else 0

        return {
            'status': 'success',
            'plaka': plaka,
            'baslangic_tarihi': baslangic_tarihi or 'Başlangıç',
            'bitis_tarihi': bitis_tarihi or 'Bugün',
            'yakit': {
                'toplam_yakit': round(toplam_yakit, 2),
                'toplam_km': round(toplam_km, 2),
                'sefer_sayisi': sefer_sayisi,
                'ort_yakit_sefer': round(ort_yakit_sefer, 2),
                'ort_birim_fiyat': round(ort_birim_fiyat, 2),
                'toplam_maliyet': round(toplam_maliyet, 2)
            },
            'tonaj': {
                'toplam_tonaj': round(toplam_tonaj, 2),
                'yuklenme_sayisi': yuklenme_sayisi,
                'ort_tonaj_yuklenme': round(ort_tonaj_yuklenme, 2)
            },
            'performans': {
                'yakit_km_orani': round(yakit_km_orani, 3),
                'km_basina_maliyet': round(km_basina_maliyet, 2),
                'ton_basina_yakit': round(ton_basina_yakit, 2),
                'verimlilik_skoru': round(verimlilik_skoru, 2)
            }
        }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }

def get_database_info():
    """Veritabanı hakkında bilgi al"""
    if not check_database_exists():
        return {
            'exists': False,
            'path': DATABASE_PATH,
            'message': 'Veritabanı dosyası bulunamadı. Lütfen önce Excel dosyalarını yükleyin.'
        }

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row['name'] for row in cursor.fetchall()]

        table_info = {}
        for table in tables:
            cursor.execute(f'SELECT COUNT(*) as count FROM {table}')
            count = cursor.fetchone()['count']
            table_info[table] = count

        conn.close()

        return {
            'exists': True,
            'path': DATABASE_PATH,
            'tables': table_info,
            'message': 'Veritabanı bağlantısı başarılı'
        }
    except Exception as e:
        return {
            'exists': False,
            'path': DATABASE_PATH,
            'error': str(e),
            'message': f'Veritabanı hatası: {str(e)}'
        }
