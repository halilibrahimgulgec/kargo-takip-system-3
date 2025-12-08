"""
Excel dosyalarını Supabase'e yükle
"""
import pandas as pd
import urllib.request
import json
import os
from datetime import datetime

# .env dosyasını manuel oku
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    return env_vars

env = load_env()
SUPABASE_URL = env.get('VITE_SUPABASE_URL')
SUPABASE_KEY = env.get('VITE_SUPABASE_ANON_KEY')

def supabase_insert_batch(table: str, data: list):
    """Supabase'e toplu veri ekle"""
    url = f'{SUPABASE_URL}/rest/v1/{table}'

    req = urllib.request.Request(url, method='POST')
    req.add_header('apikey', SUPABASE_KEY)
    req.add_header('Authorization', f'Bearer {SUPABASE_KEY}')
    req.add_header('Content-Type', 'application/json')
    req.add_header('Prefer', 'return=minimal')

    req.data = json.dumps(data).encode()

    try:
        with urllib.request.urlopen(req) as response:
            return response.status == 201
    except Exception as e:
        print(f"❌ Hata: {e}")
        return False

def delete_all_records(table: str):
    """Tablodaki tüm kayıtları sil"""
    url = f'{SUPABASE_URL}/rest/v1/{table}?id=not.is.null'

    req = urllib.request.Request(url, method='DELETE')
    req.add_header('apikey', SUPABASE_KEY)
    req.add_header('Authorization', f'Bearer {SUPABASE_KEY}')

    try:
        with urllib.request.urlopen(req) as response:
            print(f"✅ {table} tablosu temizlendi")
            return True
    except Exception as e:
        print(f"❌ Temizleme hatası: {e}")
        return False

def upload_yakit(excel_file):
    """Yakıt Excel dosyasını yükle"""
    print(f"\n⛽ Yakıt dosyası yükleniyor: {excel_file}")

    try:
        df = pd.read_excel(excel_file)
        print(f"📊 {len(df)} satır okundu")

        # Kolon isimlerini kontrol et ve düzelt
        df.columns = df.columns.str.strip().str.lower()

        # Temizlik
        delete_all_records('yakit')

        # Verileri hazırla
        records = []
        for _, row in df.iterrows():
            record = {
                'plaka': str(row.get('plaka', '')).strip() if pd.notna(row.get('plaka')) else None,
                'islem_tarihi': str(row.get('islem_tarihi', '')) if pd.notna(row.get('islem_tarihi')) else None,
                'saat': str(row.get('saat', '')) if pd.notna(row.get('saat')) else None,
                'yakit_miktari': float(row.get('yakit_miktari', 0)) if pd.notna(row.get('yakit_miktari')) else None,
                'birim_fiyat': float(row.get('birim_fiyat', 0)) if pd.notna(row.get('birim_fiyat')) else None,
                'satir_tutari': float(row.get('satir_tutari', 0)) if pd.notna(row.get('satir_tutari')) else None,
                'stok_adi': str(row.get('stok_adi', '')) if pd.notna(row.get('stok_adi')) else None,
                'km_bilgisi': float(row.get('km_bilgisi', 0)) if pd.notna(row.get('km_bilgisi')) else None
            }
            records.append(record)

        # Batch olarak yükle (1000'er 1000'er)
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            if supabase_insert_batch('yakit', batch):
                print(f"   ✅ {i+len(batch)}/{len(records)} kayıt yüklendi")
            else:
                print(f"   ❌ {i}-{i+batch_size} arası yükleme başarısız")

        print(f"✅ Yakıt verileri yüklendi: {len(records)} kayıt")
        return True

    except Exception as e:
        print(f"❌ Yakıt yükleme hatası: {e}")
        import traceback
        traceback.print_exc()
        return False

def upload_agirlik(excel_file):
    """Ağırlık Excel dosyasını yükle"""
    print(f"\n⚖️  Ağırlık dosyası yükleniyor: {excel_file}")

    try:
        df = pd.read_excel(excel_file)
        print(f"📊 {len(df)} satır okundu")

        # Kolon isimlerini kontrol et ve düzelt
        df.columns = df.columns.str.strip().str.lower()

        # Temizlik
        delete_all_records('agirlik')

        # Verileri hazırla
        records = []
        for _, row in df.iterrows():
            record = {
                'tarih': str(row.get('tarih', '')) if pd.notna(row.get('tarih')) else None,
                'miktar': float(row.get('miktar', 0)) if pd.notna(row.get('miktar')) else None,
                'birim': str(row.get('birim', '')) if pd.notna(row.get('birim')) else None,
                'net_agirlik': float(row.get('net_agirlik', 0)) if pd.notna(row.get('net_agirlik')) else None,
                'plaka': str(row.get('plaka', '')).strip() if pd.notna(row.get('plaka')) else None,
                'adres': str(row.get('adres', '')) if pd.notna(row.get('adres')) else None,
                'islem_noktasi': str(row.get('islem_noktasi', '')) if pd.notna(row.get('islem_noktasi')) else None,
                'cari_adi': str(row.get('cari_adi', '')) if pd.notna(row.get('cari_adi')) else None
            }
            records.append(record)

        # Batch olarak yükle
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            if supabase_insert_batch('agirlik', batch):
                print(f"   ✅ {i+len(batch)}/{len(records)} kayıt yüklendi")
            else:
                print(f"   ❌ {i}-{i+batch_size} arası yükleme başarısız")

        print(f"✅ Ağırlık verileri yüklendi: {len(records)} kayıt")
        return True

    except Exception as e:
        print(f"❌ Ağırlık yükleme hatası: {e}")
        import traceback
        traceback.print_exc()
        return False

def upload_arac_takip(excel_file):
    """Araç takip Excel dosyasını yükle"""
    print(f"\n🚛 Araç takip dosyası yükleniyor: {excel_file}")

    try:
        df = pd.read_excel(excel_file)
        print(f"📊 {len(df)} satır okundu")

        # Kolon isimlerini kontrol et ve düzelt
        df.columns = df.columns.str.strip().str.lower()

        # Temizlik
        delete_all_records('arac_takip')

        # Verileri hazırla
        records = []
        for _, row in df.iterrows():
            record = {
                'plaka': str(row.get('plaka', '')).strip() if pd.notna(row.get('plaka')) else None,
                'sofor_adi': str(row.get('sofor_adi', '')) if pd.notna(row.get('sofor_adi')) else None,
                'arac_gruplari': str(row.get('arac_gruplari', '')) if pd.notna(row.get('arac_gruplari')) else None,
                'tarih': str(row.get('tarih', '')) if pd.notna(row.get('tarih')) else None,
                'hareket_baslangic_tarihi': str(row.get('hareket_baslangic_tarihi', '')) if pd.notna(row.get('hareket_baslangic_tarihi')) else None,
                'hareket_bitis_tarihi': str(row.get('hareket_bitis_tarihi', '')) if pd.notna(row.get('hareket_bitis_tarihi')) else None,
                'baslangic_adresi': str(row.get('baslangic_adresi', '')) if pd.notna(row.get('baslangic_adresi')) else None,
                'bitis_adresi': str(row.get('bitis_adresi', '')) if pd.notna(row.get('bitis_adresi')) else None,
                'toplam_kilometre': float(row.get('toplam_kilometre', 0)) if pd.notna(row.get('toplam_kilometre')) else None,
                'hareket_suresi': str(row.get('hareket_suresi', '')) if pd.notna(row.get('hareket_suresi')) else None,
                'rolanti_suresi': str(row.get('rolanti_suresi', '')) if pd.notna(row.get('rolanti_suresi')) else None,
                'park_suresi': str(row.get('park_suresi', '')) if pd.notna(row.get('park_suresi')) else None,
                'gunluk_yakit_tuketimi_l': float(row.get('gunluk_yakit_tuketimi_l', 0)) if pd.notna(row.get('gunluk_yakit_tuketimi_l')) else None
            }
            records.append(record)

        # Batch olarak yükle
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            if supabase_insert_batch('arac_takip', batch):
                print(f"   ✅ {i+len(batch)}/{len(records)} kayıt yüklendi")
            else:
                print(f"   ❌ {i}-{i+batch_size} arası yükleme başarısız")

        print(f"✅ Araç takip verileri yüklendi: {len(records)} kayıt")
        return True

    except Exception as e:
        print(f"❌ Araç takip yükleme hatası: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("="*60)
    print("📤 EXCEL DOSYALARINI SUPABASE'E YÜKLE")
    print("="*60)
    print("\n📝 Kullanım:")
    print("1. Excel dosyalarınızı bu klasöre koyun")
    print("2. Dosya isimlerini girin:")
    print("\nÖrnek:")
    print("   python3 upload_excel_to_supabase.py")
    print("="*60)

    # Kullanıcıdan dosya isimleri al
    yakit_file = input("\n⛽ Yakıt Excel dosyası adı (boş bırakırsanız atlanır): ").strip()
    agirlik_file = input("⚖️  Ağırlık Excel dosyası adı (boş bırakırsanız atlanır): ").strip()
    arac_takip_file = input("🚛 Araç takip Excel dosyası adı (boş bırakırsanız atlanır): ").strip()

    print("\n" + "="*60)
    print("🚀 YÜKLEME BAŞLIYOR...")
    print("="*60)

    success_count = 0
    total_count = 0

    if yakit_file and os.path.exists(yakit_file):
        total_count += 1
        if upload_yakit(yakit_file):
            success_count += 1
    elif yakit_file:
        print(f"\n❌ Dosya bulunamadı: {yakit_file}")

    if agirlik_file and os.path.exists(agirlik_file):
        total_count += 1
        if upload_agirlik(agirlik_file):
            success_count += 1
    elif agirlik_file:
        print(f"\n❌ Dosya bulunamadı: {agirlik_file}")

    if arac_takip_file and os.path.exists(arac_takip_file):
        total_count += 1
        if upload_arac_takip(arac_takip_file):
            success_count += 1
    elif arac_takip_file:
        print(f"\n❌ Dosya bulunamadı: {arac_takip_file}")

    print("\n" + "="*60)
    print(f"✅ TAMAMLANDI: {success_count}/{total_count} dosya başarıyla yüklendi")
    print("="*60)
    print("\n🚀 Flask uygulamanızı başlatın: python app.py")
    print("="*60)
