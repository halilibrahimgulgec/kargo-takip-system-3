import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, session
from flask_cors import CORS
from datetime import datetime
import logging
from dotenv import load_dotenv
import io
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Database imports
from database import (
    get_database_info,
    get_statistics,
    get_plakalar_by_type,
    hesapla_gercek_km,
    fetch_all_paginated,
    get_aktif_kargo_araclari,
    get_aktif_binek_araclar,
    get_aktif_is_makineleri,
    get_all_araclar,
    get_all_plakas,
    add_arac,
    update_arac,
    delete_arac,
    bulk_import_araclar,
    update_arac_bulk_sahip,
    update_arac_bulk_aktif,
    get_muhasebe_data,
    supabase_insert_batch,
    record_processed_file
)

load_dotenv()

app = Flask(__name__)
CORS(app)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-here-change-in-production')

# Jinja2 template'lere Python built-in fonksiyonları ekle
app.jinja_env.globals.update(zip=zip)

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

@app.route('/health')
def health():
    """Railway health check endpoint"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()}), 200

@app.route('/')
def index():
    """Ana sayfa - Yakıt tahmin sistemi"""
    db_info = get_database_info()
    db_info['stats'] = get_statistics()
    return render_template('index.html', db_info=db_info)

@app.route('/muhasebe')
def muhasebe():
    """Muhasebe sayfası"""
    return render_template('muhasebe.html')

@app.route('/api/plakalar')
def api_plakalar():
    """Plaka listesi API - araç tipine göre filtrelenebilir"""
    try:
        arac_tipi = request.args.get('tip')
        plakalar = get_plakalar_by_type(arac_tipi)
        return jsonify({'plakalar': plakalar})
    except Exception as e:
        return jsonify({'plakalar': [], 'error': str(e)})

@app.route('/analyze', methods=['POST'])
def analyze():
    """Veritabanından analiz yap"""
    try:
        from model_analyzer import analyze_from_database
        db_info = get_database_info()
        if not db_info.get('exists'):
            flash('❌ Veritabanı bağlantısı kurulamadı!', 'error')
            return redirect(url_for('index'))

        # Filtreleri al
        baslangic_tarihi = request.form.get('baslangic_tarihi') or None
        bitis_tarihi = request.form.get('bitis_tarihi') or None
        plaka = request.form.get('plaka') or None
        dahil_taseron = request.form.get('dahil_taseron') == '1'

        # Filtreleri kaydet
        session['filter_baslangic'] = baslangic_tarihi
        session['filter_bitis'] = bitis_tarihi
        session['filter_plaka'] = plaka
        session['dahil_taseron'] = dahil_taseron

        analysis_result = analyze_from_database()

        if analysis_result['status'] == 'error':
            flash(f'❌ Veritabanı analiz hatası: {analysis_result["error"]}', 'error')
            return redirect(url_for('index'))

        if analysis_result['records'] == 0:
            flash('❌ Veritabanında hiç kayıt yok!', 'error')
            return redirect(url_for('index'))

        plakalar = []
        tahminler = []

        if analysis_result['toplam_yakit'] > 0 and len(analysis_result.get('plakalar', [])) > 0:
            # Aktif kargo araçlarını al
            aktif_kargo = get_aktif_kargo_araclari()

            # Tüm yakıt kayıtlarını al
            yakit_data = fetch_all_paginated('yakit', select='plaka,yakit_miktari',
                                            filters={'yakit_miktari': 'not.is.null', 'yakit_miktari': 'gt.0'})

            # Plaka bazında yakıt topla
            plaka_yakit = {}
            for row in yakit_data:
                plaka_str = str(row['plaka'])
                yakit = float(row['yakit_miktari']) if row.get('yakit_miktari') else 0
                if yakit > 0:
                    plaka_yakit[plaka_str] = plaka_yakit.get(plaka_str, 0) + yakit

            # Her plaka için tahmin yap
            for plaka_str, toplam_yakit in plaka_yakit.items():
                # Gerçek km hesapla
                gercek_km = hesapla_gercek_km(plaka_str, baslangic_tarihi, bitis_tarihi)

                # Gerçek ortalama hesapla
                if gercek_km > 0:
                    gercek_ort = toplam_yakit / (gercek_km / 100)
                else:
                    gercek_ort = 0

                # Aktif mi?
                aktif_mi = plaka_str in aktif_kargo

                plakalar.append(plaka_str)
                tahminler.append({
                    'plaka': plaka_str,
                    'toplam_yakit': round(toplam_yakit, 2),
                    'gercek_km': round(gercek_km, 2),
                    'gercek_ortalama': round(gercek_ort, 2),
                    'tahmini_ortalama': round(gercek_ort, 2) if gercek_km > 0 else 0,
                    'aktif': aktif_mi
                })

            # Sıralama
            tahminler.sort(key=lambda x: x['toplam_yakit'], reverse=True)

        return render_template('result.html',
                             result=analysis_result,
                             plakalar=plakalar,
                             tahminler=tahminler,
                             filters={
                                 'baslangic': baslangic_tarihi,
                                 'bitis': bitis_tarihi,
                                 'plaka': plaka,
                                 'dahil_taseron': dahil_taseron
                             })

    except Exception as e:
        logger.error(f"Analyze error: {str(e)}")
        flash(f'❌ Analiz sırasında hata oluştu: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/kargo_arac_filtre')
def kargo_arac_filtre():
    """Kargo araçları filtre sayfası"""
    aktif_araclar = get_aktif_kargo_araclari()
    return render_template('kargo_arac_filtre.html', aktif_araclar=aktif_araclar, toplam=len(aktif_araclar))

@app.route('/binek_arac_filtre')
def binek_arac_filtre():
    """Binek araçları filtre sayfası"""
    dahil_taseron = request.args.get('dahil_taseron') == '1'
    aktif_araclar = get_aktif_binek_araclar(dahil_taseron)

    return render_template('binek_arac_filtre.html',
                         aktif_araclar=aktif_araclar,
                         toplam=len(aktif_araclar),
                         dahil_taseron=dahil_taseron)

@app.route('/is_makinesi_filtre')
def is_makinesi_filtre():
    """İş makineleri filtre sayfası"""
    dahil_taseron = request.args.get('dahil_taseron') == '1'
    aktif_araclar = get_aktif_is_makineleri(dahil_taseron)

    return render_template('is_makinesi_filtre.html',
                         aktif_araclar=aktif_araclar,
                         toplam=len(aktif_araclar),
                         dahil_taseron=dahil_taseron)

@app.route('/arac_yonetimi')
def arac_yonetimi():
    """Araç yönetimi sayfası"""
    araclar = get_all_araclar()
    return render_template('arac_yonetimi.html', araclar=araclar)

@app.route('/api/araclar')
def api_araclar():
    """Araç listesi API"""
    try:
        araclar = get_all_araclar()
        return jsonify({'status': 'success', 'araclar': araclar})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/arac/ekle', methods=['POST'])
def api_arac_ekle():
    """Yeni araç ekle"""
    try:
        data = request.get_json()
        result = add_arac(
            plaka=data['plaka'],
            sahip=data['sahip'],
            arac_tipi=data['arac_tipi'],
            notlar=data.get('notlar', '')
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/arac/guncelle', methods=['POST'])
def api_arac_guncelle():
    """Araç güncelle"""
    try:
        data = request.get_json()
        result = update_arac(
            plaka=data['plaka'],
            sahip=data['sahip'],
            arac_tipi=data['arac_tipi'],
            aktif=data['aktif'],
            notlar=data.get('notlar', '')
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/arac/sil', methods=['POST'])
def api_arac_sil():
    """Araç sil"""
    try:
        data = request.get_json()
        result = delete_arac(data['plaka'])
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/araclar/toplu_ekle', methods=['POST'])
def api_araclar_toplu_ekle():
    """Tüm plakaları toplu olarak araçlar tablosuna ekle"""
    try:
        result = bulk_import_araclar()
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/araclar/toplu_guncelle/sahip', methods=['POST'])
def api_araclar_toplu_sahip():
    """Toplu araç sahip güncelle"""
    try:
        data = request.get_json()
        basarili = update_arac_bulk_sahip(data['plakalar'], data['sahip'])
        return jsonify({'status': 'success', 'basarili': basarili})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/araclar/toplu_guncelle/aktif', methods=['POST'])
def api_araclar_toplu_aktif():
    """Toplu araç aktif/pasif güncelle"""
    try:
        data = request.get_json()
        basarili = update_arac_bulk_aktif(data['plakalar'], data['aktif'])
        return jsonify({'status': 'success', 'basarili': basarili})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/muhasebe/hesapla', methods=['POST'])
def api_muhasebe_hesapla():
    """Muhasebe hesaplama API"""
    try:
        data = request.get_json()

        baslangic = data.get('baslangic_tarihi')
        bitis = data.get('bitis_tarihi')
        plaka = data.get('plaka')

        result = get_muhasebe_data(baslangic, bitis, plaka)
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/muhasebe/rapor', methods=['POST'])
def muhasebe_rapor():
    """Muhasebe raporu oluştur"""
    try:
        baslangic_tarihi = request.form.get('baslangic_tarihi')
        bitis_tarihi = request.form.get('bitis_tarihi')
        plaka = request.form.get('plaka') or None

        result = get_muhasebe_data(baslangic_tarihi, bitis_tarihi, plaka)

        if result['status'] == 'error':
            flash(f'❌ Hata: {result["message"]}', 'error')
            return redirect(url_for('muhasebe'))

        return render_template('muhasebe_result.html',
                             result=result,
                             baslangic=baslangic_tarihi,
                             bitis=bitis_tarihi,
                             plaka=plaka)
    except Exception as e:
        logger.error(f"Muhasebe rapor hatası: {str(e)}")
        flash(f'❌ Rapor oluşturulurken hata: {str(e)}', 'error')
        return redirect(url_for('muhasebe'))

@app.route('/muhasebe/export_pdf', methods=['POST'])
def muhasebe_export_pdf():
    """Muhasebe raporunu PDF olarak indir"""
    try:
        data = request.get_json()
        baslangic = data.get('baslangic_tarihi')
        bitis = data.get('bitis_tarihi')
        plaka = data.get('plaka')

        result = get_muhasebe_data(baslangic, bitis, plaka)

        if result['status'] == 'error':
            return jsonify({'status': 'error', 'message': result['message']})

        # PDF oluştur
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(A4))
        elements = []

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#2c3e50'),
            spaceAfter=30,
            alignment=1
        )

        # Başlık
        title = Paragraph("MUHASEBE RAPORU", title_style)
        elements.append(title)
        elements.append(Spacer(1, 20))

        # Özet bilgiler
        ozet_data = [
            ['Toplam Gelir', f"{result['toplam_gelir']:,.2f} ₺"],
            ['Toplam Gider', f"{result['toplam_gider']:,.2f} ₺"],
            ['Net Kar', f"{result['net_kar']:,.2f} ₺"],
            ['Kar Marjı', f"{result['kar_marji']:.2f}%"]
        ]

        ozet_table = Table(ozet_data, colWidths=[10*cm, 10*cm])
        ozet_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 12),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))

        elements.append(ozet_table)
        elements.append(Spacer(1, 30))

        # Plaka bazlı detay
        if result.get('plaka_bazli'):
            plaka_baslik = Paragraph("Plaka Bazlı Detay", styles['Heading2'])
            elements.append(plaka_baslik)
            elements.append(Spacer(1, 10))

            plaka_data = [['Plaka', 'Gelir (₺)', 'Gider (₺)', 'Kar (₺)']]

            for plaka_str, values in result['plaka_bazli'].items():
                plaka_data.append([
                    plaka_str,
                    f"{values['gelir']:,.2f}",
                    f"{values['gider']:,.2f}",
                    f"{values['kar']:,.2f}"
                ])

            plaka_table = Table(plaka_data, colWidths=[5*cm, 5*cm, 5*cm, 5*cm])
            plaka_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))

            elements.append(plaka_table)

        doc.build(elements)
        buffer.seek(0)

        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'muhasebe_rapor_{datetime.now().strftime("%Y%m%d")}.pdf'
        )

    except Exception as e:
        logger.error(f"PDF export error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/performans_analizi')
def performans_analizi():
    """Performans analizi sayfası"""
    plakalar = get_all_plakas()
    return render_template('performans_analizi.html', plakalar=plakalar)

@app.route('/api/performans/hesapla', methods=['POST'])
def api_performans_hesapla():
    """Performans analizi hesaplama API"""
    try:
        data = request.get_json()
        plaka = data.get('plaka')
        baslangic = data.get('baslangic_tarihi')
        bitis = data.get('bitis_tarihi')

        # Yakıt verilerini al
        filters = {'plaka': f'eq.{plaka}'}
        if baslangic:
            filters['islem_tarihi'] = f'gte.{baslangic}'
        if bitis:
            filters['islem_tarihi'] = f'lte.{bitis}'

        yakit_data = fetch_all_paginated('yakit', filters=filters, order='islem_tarihi.asc')

        # Hesaplamalar
        toplam_yakit = sum(float(row.get('yakit_miktari', 0) or 0) for row in yakit_data)
        toplam_maliyet = sum(float(row.get('satir_tutari', 0) or 0) for row in yakit_data)
        gercek_km = hesapla_gercek_km(plaka, baslangic, bitis)

        if gercek_km > 0 and toplam_yakit > 0:
            ort_tuketim = toplam_yakit / (gercek_km / 100)
        else:
            ort_tuketim = 0

        # Aylık dağılım
        aylik_dagilim = {}
        for row in yakit_data:
            tarih = row.get('islem_tarihi', '')[:7]  # YYYY-MM
            if tarih:
                if tarih not in aylik_dagilim:
                    aylik_dagilim[tarih] = {'yakit': 0, 'maliyet': 0}
                aylik_dagilim[tarih]['yakit'] += float(row.get('yakit_miktari', 0) or 0)
                aylik_dagilim[tarih]['maliyet'] += float(row.get('satir_tutari', 0) or 0)

        return jsonify({
            'status': 'success',
            'plaka': plaka,
            'toplam_yakit': round(toplam_yakit, 2),
            'toplam_maliyet': round(toplam_maliyet, 2),
            'gercek_km': round(gercek_km, 2),
            'ortalama_tuketim': round(ort_tuketim, 2),
            'aylik_dagilim': aylik_dagilim,
            'kayit_sayisi': len(yakit_data)
        })

    except Exception as e:
        logger.error(f"Performans hesaplama hatası: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/performans_detay/<plaka>')
def performans_detay(plaka):
    """Performans detay sayfası"""
    try:
        # Araç bilgisi
        arac_data = fetch_all_paginated('araclar', filters={'plaka': f'eq.{plaka}'})
        arac = arac_data[0] if arac_data else {'plaka': plaka, 'arac_tipi': 'Bilinmiyor'}

        # Yakıt verileri
        yakit_data = fetch_all_paginated('yakit',
                                        filters={'plaka': f'eq.{plaka}'},
                                        order='islem_tarihi.desc')

        # Temel metrikler
        toplam_yakit = sum(float(row.get('yakit_miktari', 0) or 0) for row in yakit_data)
        toplam_maliyet = sum(float(row.get('satir_tutari', 0) or 0) for row in yakit_data)
        gercek_km = hesapla_gercek_km(plaka)

        if gercek_km > 0:
            ort_tuketim = toplam_yakit / (gercek_km / 100)
        else:
            ort_tuketim = 0

        return render_template('performans_detay.html',
                             arac=arac,
                             yakit_kayitlari=yakit_data[:50],  # Son 50 kayıt
                             toplam_yakit=round(toplam_yakit, 2),
                             toplam_maliyet=round(toplam_maliyet, 2),
                             gercek_km=round(gercek_km, 2),
                             ortalama_tuketim=round(ort_tuketim, 2))
    except Exception as e:
        logger.error(f"Performans detay hatası: {str(e)}")
        flash(f'❌ Hata: {str(e)}', 'error')
        return redirect(url_for('performans_analizi'))

@app.route('/performans_karsilastirma')
def performans_karsilastirma():
    """Performans karşılaştırma sayfası"""
    plakalar = get_all_plakas()
    return render_template('performans_karsilastirma.html', plakalar=plakalar)

@app.route('/api/performans/karsilastir', methods=['POST'])
def api_performans_karsilastir():
    """Çoklu araç performans karşılaştırma"""
    try:
        data = request.get_json()
        plakalar = data.get('plakalar', [])
        baslangic = data.get('baslangic_tarihi')
        bitis = data.get('bitis_tarihi')

        sonuclar = []

        for plaka in plakalar:
            filters = {'plaka': f'eq.{plaka}'}
            if baslangic:
                filters['islem_tarihi'] = f'gte.{baslangic}'
            if bitis:
                filters['islem_tarihi'] = f'lte.{bitis}'

            yakit_data = fetch_all_paginated('yakit', filters=filters)

            toplam_yakit = sum(float(row.get('yakit_miktari', 0) or 0) for row in yakit_data)
            toplam_maliyet = sum(float(row.get('satir_tutari', 0) or 0) for row in yakit_data)
            gercek_km = hesapla_gercek_km(plaka, baslangic, bitis)

            if gercek_km > 0:
                ort_tuketim = toplam_yakit / (gercek_km / 100)
            else:
                ort_tuketim = 0

            sonuclar.append({
                'plaka': plaka,
                'toplam_yakit': round(toplam_yakit, 2),
                'toplam_maliyet': round(toplam_maliyet, 2),
                'gercek_km': round(gercek_km, 2),
                'ortalama_tuketim': round(ort_tuketim, 2)
            })

        return jsonify({'status': 'success', 'sonuclar': sonuclar})

    except Exception as e:
        logger.error(f"Karşılaştırma hatası: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/veri_yukleme')
def veri_yukleme():
    """Veri yükleme sayfası"""
    return render_template('veri_yukleme.html')

@app.route('/api/veri_yukle', methods=['POST'])
def api_veri_yukle():
    """Excel dosyasını Supabase'e yükle"""
    try:
        if 'file' not in request.files:
            return jsonify({'status': 'error', 'message': 'Dosya seçilmedi'})

        file = request.files['file']
        tablo = request.form.get('tablo', 'yakit')

        if file.filename == '':
            return jsonify({'status': 'error', 'message': 'Dosya adı boş'})

        # Excel dosyasını oku
        df = pd.read_excel(file)

        # Sütun adlarını küçük harfe çevir ve boşlukları alt çizgi ile değiştir
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        # NaN değerleri None'a çevir
        df = df.where(pd.notnull(df), None)

        # Dictionary listesine çevir
        records = df.to_dict('records')

        # Batch olarak Supabase'e yükle
        batch_size = 100
        total_uploaded = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            success = supabase_insert_batch(tablo, batch)
            if success:
                total_uploaded += len(batch)

        # İşlem kaydı tut
        record_processed_file(file.filename, tablo, total_uploaded)

        return jsonify({
            'status': 'success',
            'message': f'{total_uploaded} kayıt başarıyla yüklendi',
            'uploaded': total_uploaded
        })

    except Exception as e:
        logger.error(f"Veri yükleme hatası: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/ai_assistant')
def ai_assistant():
    """AI Asistan sayfası"""
    return render_template('ai_assistant.html')

@app.route('/ai_analysis')
def ai_analysis():
    """AI Analiz sayfası"""
    plakalar = get_all_plakas()
    return render_template('ai_analysis.html', plakalar=plakalar)

@app.route('/anomaly_dashboard')
def anomaly_dashboard():
    """Anomali tespit dashboard"""
    plakalar = get_all_plakas()
    return render_template('anomaly_dashboard.html', plakalar=plakalar)

@app.route('/api/ai/predict', methods=['POST'])
def api_ai_predict():
    """AI tahmin API"""
    try:
        from ai_model import predict_fuel_consumption

        data = request.get_json()
        result = predict_fuel_consumption(
            plaka=data.get('plaka'),
            gun_sayisi=int(data.get('gun_sayisi', 30))
        )

        return jsonify(result)

    except Exception as e:
        logger.error(f"AI prediction error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/ai/anomaly_detect', methods=['POST'])
def api_ai_anomaly():
    """AI anomali tespit API"""
    try:
        from ai_model import detect_anomalies

        data = request.get_json()
        result = detect_anomalies(
            plaka=data.get('plaka'),
            baslangic_tarihi=data.get('baslangic_tarihi'),
            bitis_tarihi=data.get('bitis_tarihi')
        )

        return jsonify(result)

    except Exception as e:
        logger.error(f"AI anomaly detection error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/ai/bulk_predict', methods=['POST'])
def api_ai_bulk_predict():
    """Toplu AI tahmin API"""
    try:
        from ai_model import bulk_predict_all_vehicles

        data = request.get_json()
        result = bulk_predict_all_vehicles(gun_sayisi=int(data.get('gun_sayisi', 30)))

        return jsonify(result)

    except Exception as e:
        logger.error(f"AI bulk prediction error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/ai/chat', methods=['POST'])
def api_ai_chat():
    """AI Asistan chat API"""
    try:
        from ollama_assistant import get_assistant_response

        data = request.get_json()
        question = data.get('question', '')
        context = data.get('context', {})

        response = get_assistant_response(question, context)
        return jsonify({'status': 'success', 'response': response})

    except Exception as e:
        logger.error(f"AI chat error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/binek-arac-analizi', methods=['GET', 'POST'])
def binek_arac_analizi():
    """Binek araç analizi"""
    try:
        from database import get_aktif_binek_araclar, get_yakit_by_plaka, hesapla_gercek_km

        baslangic_tarihi = request.form.get('baslangic_tarihi') if request.method == 'POST' else None
        bitis_tarihi = request.form.get('bitis_tarihi') if request.method == 'POST' else None
        plaka_filtre = request.form.get('plaka') if request.method == 'POST' else None
        dahil_taseron = request.form.get('dahil_taseron') == '1' if request.method == 'POST' else False

        aktif_binek = get_aktif_binek_araclar(dahil_taseron=dahil_taseron)

        if not aktif_binek:
            flash('⚠️ Aktif binek araç bulunamadı', 'warning')
            return render_template('result.html', arac_detaylari=[], genel_ozet={'arac_tipi': 'Binek Araç', 'toplam_arac': 0, 'toplam_yakit': 0})

        arac_detaylari = []
        toplam_yakit_genel = 0

        for plaka in aktif_binek:
            if plaka_filtre and plaka != plaka_filtre:
                continue

            yakit_data = get_yakit_by_plaka(plaka)

            if baslangic_tarihi or bitis_tarihi:
                yakit_data = [row for row in yakit_data
                            if (not baslangic_tarihi or row.get('islem_tarihi', '') >= baslangic_tarihi)
                            and (not bitis_tarihi or row.get('islem_tarihi', '') <= bitis_tarihi)]

            toplam_yakit = sum(float(row.get('yakit_miktari', 0) or 0) for row in yakit_data)
            yakit_alimlari = len(yakit_data)
            ortalama_yakit = toplam_yakit / yakit_alimlari if yakit_alimlari > 0 else 0

            toplam_km = hesapla_gercek_km(plaka, baslangic_tarihi, bitis_tarihi)
            tuketim = (toplam_yakit / toplam_km * 100) if toplam_km > 0 else 0

            arac_detaylari.append({
                'plaka': plaka,
                'toplam_yakit': toplam_yakit,
                'toplam_km': toplam_km,
                'ortalama_yakit': ortalama_yakit,
                'yakit_alimlari': yakit_alimlari,
                'tuketim_100km': tuketim
            })

            toplam_yakit_genel += toplam_yakit

        genel_ozet = {
            'toplam_arac': len(arac_detaylari),
            'toplam_yakit': toplam_yakit_genel,
            'arac_tipi': 'Binek Araç'
        }

        plakalar = [arac['plaka'] for arac in arac_detaylari]
        tahminler = [round(arac['ortalama_yakit'], 2) for arac in arac_detaylari]
        toplam_yakit_alimlari = sum(arac['yakit_alimlari'] for arac in arac_detaylari)

        return render_template('result.html',
                             arac_detaylari=arac_detaylari,
                             genel_ozet=genel_ozet,
                             analiz_tipi='binek',
                             sefer=toplam_yakit_alimlari,
                             yakit=round(toplam_yakit_genel, 2),
                             ortalama_tahmin=round(toplam_yakit_genel / toplam_yakit_alimlari, 2) if toplam_yakit_alimlari > 0 else 0,
                             plakalar=plakalar,
                             tahminler=tahminler)
    except Exception as e:
        logger.error(f"Binek araç analizi hatası: {str(e)}")
        flash(f'❌ Hata: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/is-makinesi-analizi', methods=['GET', 'POST'])
def is_makinesi_analizi():
    """İş makinesi analizi"""
    try:
        from database import get_aktif_is_makineleri, get_yakit_by_plaka, hesapla_gercek_km

        baslangic_tarihi = request.form.get('baslangic_tarihi') if request.method == 'POST' else None
        bitis_tarihi = request.form.get('bitis_tarihi') if request.method == 'POST' else None
        plaka_filtre = request.form.get('plaka') if request.method == 'POST' else None
        dahil_taseron = request.form.get('dahil_taseron') == '1' if request.method == 'POST' else False

        aktif_makineler = get_aktif_is_makineleri(dahil_taseron=dahil_taseron)

        if not aktif_makineler:
            flash('⚠️ Aktif iş makinesi bulunamadı', 'warning')
            return render_template('result.html', arac_detaylari=[], genel_ozet={'arac_tipi': 'İş Makinesi', 'toplam_arac': 0, 'toplam_yakit': 0})

        arac_detaylari = []
        toplam_yakit_genel = 0

        for plaka in aktif_makineler:
            if plaka_filtre and plaka != plaka_filtre:
                continue

            yakit_data = get_yakit_by_plaka(plaka)

            if baslangic_tarihi or bitis_tarihi:
                yakit_data = [row for row in yakit_data
                            if (not baslangic_tarihi or row.get('islem_tarihi', '') >= baslangic_tarihi)
                            and (not bitis_tarihi or row.get('islem_tarihi', '') <= bitis_tarihi)]

            toplam_yakit = sum(float(row.get('yakit_miktari', 0) or 0) for row in yakit_data)
            yakit_alimlari = len(yakit_data)
            ortalama_yakit = toplam_yakit / yakit_alimlari if yakit_alimlari > 0 else 0

            toplam_km = hesapla_gercek_km(plaka, baslangic_tarihi, bitis_tarihi)
            tuketim = (toplam_yakit / toplam_km * 100) if toplam_km > 0 else 0

            arac_detaylari.append({
                'plaka': plaka,
                'toplam_yakit': toplam_yakit,
                'toplam_km': toplam_km,
                'ortalama_yakit': ortalama_yakit,
                'yakit_alimlari': yakit_alimlari,
                'tuketim_100km': tuketim
            })

            toplam_yakit_genel += toplam_yakit

        genel_ozet = {
            'toplam_arac': len(arac_detaylari),
            'toplam_yakit': toplam_yakit_genel,
            'arac_tipi': 'İş Makinesi'
        }

        plakalar = [arac['plaka'] for arac in arac_detaylari]
        tahminler = [round(arac['ortalama_yakit'], 2) for arac in arac_detaylari]
        toplam_yakit_alimlari = sum(arac['yakit_alimlari'] for arac in arac_detaylari)

        return render_template('result.html',
                             arac_detaylari=arac_detaylari,
                             genel_ozet=genel_ozet,
                             analiz_tipi='is_makinesi',
                             sefer=toplam_yakit_alimlari,
                             yakit=round(toplam_yakit_genel, 2),
                             ortalama_tahmin=round(toplam_yakit_genel / toplam_yakit_alimlari, 2) if toplam_yakit_alimlari > 0 else 0,
                             plakalar=plakalar,
                             tahminler=tahminler)
    except Exception as e:
        logger.error(f"İş makinesi analizi hatası: {str(e)}")
        flash(f'❌ Hata: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/export-excel', methods=['POST'])
def export_excel():
    """Analiz sonuçlarını Excel'e dönüştür"""
    try:
        data = request.get_json()
        arac_detaylari = data.get('arac_detaylari', [])

        if not arac_detaylari:
            return jsonify({'status': 'error', 'message': 'Veri bulunamadı'}), 400

        excel_data = []
        for arac in arac_detaylari:
            row = {'Plaka': arac.get('plaka', ''), 'Toplam Yakıt (L)': arac.get('toplam_yakit', 0)}

            if 'sefer_sayisi' in arac:
                row['Toplam KM'] = arac.get('toplam_km', 0) or 0
                row['Toplam Sefer'] = arac.get('sefer_sayisi', 0)
                row['KG Toplam'] = arac.get('kg_toplam', 0) or 0
                row['Ortalama Yakıt (L)'] = arac.get('ortalama_yakit', 0)
                row['KM/Litre'] = arac.get('km_litre_orani', 0) or 0
            else:
                row['Toplam KM'] = arac.get('toplam_km', 0) or 0
                row['Yakıt Alımları'] = arac.get('yakit_alimlari', 0)
                row['Ortalama Yakıt (L)'] = arac.get('ortalama_yakit', 0)
                row['Tüketim (L/100km)'] = arac.get('tuketim_100km', 0) or 0

            excel_data.append(row)

        df = pd.DataFrame(excel_data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Analiz Sonuçları', index=False)

            workbook = writer.book
            worksheet = writer.sheets['Analiz Sonuçları']

            header_format = workbook.add_format({'bold': True, 'bg_color': '#4CAF50', 'font_color': 'white', 'border': 1, 'align': 'center'})
            number_format = workbook.add_format({'num_format': '#,##0.00'})

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, 18)

                if col_num > 0:
                    for row_num in range(1, len(df) + 1):
                        worksheet.write(row_num, col_num, df.iloc[row_num-1, col_num], number_format)

        output.seek(0)
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        as_attachment=True, download_name=f'yakit_analizi_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
    except Exception as e:
        logger.error(f"Excel export error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/export-pdf', methods=['POST'])
def export_pdf():
    """Analiz sonuçlarını PDF'e dönüştür"""
    try:
        data = request.get_json()
        arac_detaylari = data.get('arac_detaylari', [])
        analiz_tipi = data.get('analiz_tipi', '')

        if not arac_detaylari:
            return jsonify({'status': 'error', 'message': 'Veri bulunamadı'}), 400

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
        elements = []

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=18,
                                     textColor=colors.HexColor('#2C3E50'), spaceAfter=20)

        elements.append(Paragraph('Yakıt Analiz Raporu', title_style))
        elements.append(Spacer(1, 0.3*cm))
        elements.append(Paragraph(f'Tarih: {datetime.now().strftime("%d.%m.%Y %H:%M")}', styles['Normal']))
        elements.append(Spacer(1, 0.8*cm))

        is_kargo = any('sefer_sayisi' in arac for arac in arac_detaylari)

        if is_kargo:
            table_data = [['#', 'Plaka', 'Yakıt (L)', 'KM', 'Sefer', 'KG', 'KM/L']]
            for idx, arac in enumerate(arac_detaylari, 1):
                table_data.append([str(idx), arac.get('plaka', ''), f"{arac.get('toplam_yakit', 0):.1f}",
                                 f"{arac.get('toplam_km', 0):.0f}" if arac.get('toplam_km', 0) > 0 else '-',
                                 str(arac.get('sefer_sayisi', 0)),
                                 f"{arac.get('kg_toplam', 0):.0f}" if arac.get('kg_toplam', 0) > 0 else '-',
                                 f"{arac.get('km_litre_orani', 0):.2f}" if arac.get('km_litre_orani', 0) > 0 else '-'])

            table = Table(table_data, colWidths=[1*cm, 3*cm, 2.2*cm, 2*cm, 1.8*cm, 2.2*cm, 2*cm])
        else:
            arac_tipi = 'İş Makinesi' if analiz_tipi == 'is_makinesi' else 'Binek Araç'
            table_data = [['#', 'Plaka', 'Toplam Yakıt (L)', 'Toplam KM', 'Yakıt Alımları', 'Tüketim (L/100km)']]
            for idx, arac in enumerate(arac_detaylari, 1):
                table_data.append([str(idx), arac.get('plaka', ''), f"{arac.get('toplam_yakit', 0):.2f}",
                                 f"{arac.get('toplam_km', 0):.0f}" if arac.get('toplam_km', 0) > 0 else '-',
                                 str(arac.get('yakit_alimlari', 0)),
                                 f"{arac.get('tuketim_100km', 0):.2f}" if arac.get('tuketim_100km', 0) > 0 else '-'])

            table = Table(table_data, colWidths=[1*cm, 3.5*cm, 3.5*cm, 3*cm, 3*cm, 3.5*cm])

        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
        ]))

        elements.append(table)
        doc.build(elements)
        buffer.seek(0)

        return send_file(buffer, mimetype='application/pdf', as_attachment=True,
                        download_name=f'yakit_analizi_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf')
    except Exception as e:
        logger.error(f"PDF export error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint for Railway"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
