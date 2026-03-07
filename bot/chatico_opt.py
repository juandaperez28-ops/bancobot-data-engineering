import time
import dask.dataframe as dd
import s3fs
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib import colors
from datetime import datetime
import pytz
import os


MINIO_ENDPOINT = "http://minio:9000"

storage_options = {
    "key": "admin",
    "secret": "admin123",
    "client_kwargs": {
        "endpoint_url": MINIO_ENDPOINT
    },
    "config_kwargs": {
        "s3": {"addressing_style": "path"}
    }
}


def plantilla_pdf(filename, titulo, elementos):

    styles = getSampleStyleSheet()
    styles = getSampleStyleSheet()

    styles["Title"].fontName = "Helvetica-Bold"
    styles["Heading1"].fontName = "Helvetica-Bold"
    styles["Normal"].fontName = "Helvetica"

    # Ajustar tamaños de texto
    styles["Title"].fontSize = 18
    styles["Title"].leading = 20

    styles["Heading1"].fontSize = 16
    styles["Heading1"].spaceAfter = 6

    styles["Normal"].fontSize = 9

    story = []

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    logo_path = os.path.join(BASE_DIR, "..", "assets", "LOGO_BANCOBOT.png")
    # ==============================
    # HEADER
    # ==============================

    logo = Image(logo_path, width=160, height=100)

    header = Table([
        [logo, Paragraph("<b>BancoBot</b><br/>Servicios Financieros", styles["Title"])]
    ], colWidths=[5*cm, 12*cm])

    header.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE")
    ]))

    story.append(header)
    story.append(Spacer(1, 2))

    # Línea separadora
    linea = Table([[""]], colWidths=[17*cm])
    linea.setStyle(TableStyle([
        ("LINEBELOW", (0,0), (-1,-1), 2, colors.darkblue)
    ]))

    story.append(linea)
    story.append(Spacer(1, 8))

    # ==============================
    # TITULO
    # ==============================

    story.append(Paragraph(f"<b>{titulo}</b>", styles["Heading1"]))
    story.append(Spacer(1, 10))

    # ==============================
    # CONTENIDO
    # ==============================

    story.extend(elementos)
    story.append(Spacer(1, 15))

    # ==============================
    # FOOTER
    # ==============================

    footer = Table([
        ["Documento generado automáticamente por BancoBot"]
    ], colWidths=[17*cm])

    footer.setStyle(TableStyle([
        ("ALIGN",(0,0),(-1,-1),"CENTER"),
        ("TEXTCOLOR",(0,0),(-1,-1),colors.grey),
        ("FONTSIZE",(0,0),(-1,-1),8)
    ]))

    story.append(footer)

    pdf = SimpleDocTemplate(
        filename,
        pagesize=letter,
        leftMargin=2*cm,
        rightMargin=2*cm,
        topMargin=1.5*cm,
        bottomMargin=1.5*cm
    )

    pdf.build(story)

    return filename



# ==============================
# CONTENIDO EXTRACTO
# ==============================

def contenido_extracto(df):

    if hasattr(df, "compute"):
        df = df.compute()

    data = [df.columns.tolist()] + df.values.tolist()

    tabla = Table(data, repeatRows=1, hAlign="CENTER")

    tabla.setStyle(TableStyle([

        ("BACKGROUND",(0,0),(-1,0),colors.darkblue),
        ("TEXTCOLOR",(0,0),(-1,0),colors.white),

        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),

        ("ALIGN",(0,0),(-1,-1),"CENTER"),

        ("GRID",(0,0),(-1,-1),0.5,colors.grey),

        ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.whitesmoke, colors.lightgrey]),

        ("FONTSIZE",(0,0),(-1,-1),9),

        ("LEFTPADDING",(0,0),(-1,-1),4),
        ("RIGHTPADDING",(0,0),(-1,-1),4),
        ("TOPPADDING",(0,0),(-1,-1),2),
        ("BOTTOMPADDING",(0,0),(-1,-1),2)

    ]))

    return [tabla]


def generar_pdf_extracto(df):

    elementos = contenido_extracto(df)

    return plantilla_pdf(
        "extracto.pdf",
        "Extracto Bancario",
        elementos
    )


# ==============================
# CONTENIDO CERTIFICADO
# ==============================

def contenido_certificado(nombre, cuenta):

    colombia = pytz.timezone("America/Bogota")
    fecha = datetime.now(colombia).strftime("%Y/%m/%d %H:%M")

    styles = getSampleStyleSheet()
    styles["Normal"].fontSize = 10

    texto = f"""
    <para align="justify">

    <b>BANCOBOT S.A.</b>, entidad financiera identificada con NIT 
    <b>900.000.000-1</b>, con domicilio principal en la ciudad de 
    <b>Bogotá D.C., República de Colombia</b>, certifica que:

    <br/><br/>

    El(la) señor(a) <b>{nombre}</b> es titular de la cuenta bancaria 
    número <b>{cuenta}</b> en nuestra entidad.

    <br/><br/>

    La presente certificación se expide a solicitud del interesado 
    para los fines que estime convenientes.

    <br/><br/>

    Este documento ha sido generado automáticamente por el sistema 
    <b>BancoBot</b> y tiene plena validez informativa dentro del 
    marco del proyecto académico en el cual se desarrolla esta 
    plataforma tecnológica.

    <br/><br/><br/>

    Se expide el {fecha} en la ciudad de <b>Bogotá D.C.</b>, República de Colombia.

    </para>
    """
    return [Paragraph(texto, styles["Normal"])]


def generar_pdf_certificado(nombre, cuenta):

    elementos = contenido_certificado(nombre, cuenta)

    return plantilla_pdf(
        "certificado.pdf",
        "Certificado Bancario",
        elementos
    )

def EXTRACT_BANC(username, mes, STEP):

    # =========================
    # Obtener user_id
    # =========================
    df_users = dd.read_parquet(
        "s3://realbucket/USERS/",
        columns=["username", "usuario_id"],
        storage_options=storage_options,
        engine="pyarrow"
    )

    user = df_users[df_users["username"] == username].compute()
    user_id = user.iloc[0]["usuario_id"]

    # =========================
    # STEP 1 → Obtener meses
    # =========================
    if STEP == 1:

        df_txn = dd.read_parquet(
            "s3://realbucket/movimientos_parquet/",
            columns=["usuario_id", "txn_date"],
            storage_options=storage_options,
            engine="pyarrow"
        )

        df_user_txn = df_txn[df_txn["usuario_id"] == user_id]
        df_user_txn["txn_date"] = dd.to_datetime(df_user_txn["txn_date"], errors="coerce")
        df_user_txn["year_month"] = df_user_txn["txn_date"].dt.strftime("%Y-%m")

        meses_disponibles = (
            df_user_txn["year_month"]
            .drop_duplicates()
            .compute()
            .sort_values()
            .tolist()
        )

        return meses_disponibles

    # =========================
    # STEP 2 → Extracto del mes
    # =========================
    elif STEP == 2:

        df_txn = dd.read_parquet(
            "s3://realbucket/movimientos_parquet/",
            storage_options=storage_options,
            engine="pyarrow"
        )

        df_txn["txn_date"] = dd.to_datetime(df_txn["txn_date"])

        inicio = pd.to_datetime(f"{mes}-01")
        fin = inicio + pd.offsets.MonthEnd(1)

        df_user_txn = df_txn[
            (df_txn["usuario_id"] == user_id) &
            (df_txn["txn_date"] >= inicio) &
            (df_txn["txn_date"] <= fin)
        ]

        return df_user_txn

def OBTENER_CUENTA(username):

    storage_options = {
        "key": "admin",
        "secret": "admin123",
        "client_kwargs": {
            "endpoint_url": "http://minio:9000"
        },
        "config_kwargs": {
            "s3": {"addressing_style": "path"}
        }
    }

    df_users = dd.read_parquet(
        "s3://realbucket/USERS/",
        storage_options=storage_options
    )

    cuenta = (
        df_users[df_users["username"] == username]["main_account"]
        .compute()
        .iloc[0]
    )

    return cuenta

def RESUMEN_CUENTA(username):

    storage_options = {
        "key": "admin",
        "secret": "admin123",
        "client_kwargs": {
            "endpoint_url": "http://minio:9000"
        },
        "config_kwargs": {
            "s3": {"addressing_style": "path"}
        }
    }

    df = dd.read_parquet(
        "s3://realbucket/movimientos_parquet/",
        storage_options=storage_options,
        engine="pyarrow"
    )

    df_users = dd.read_parquet(
        "s3://realbucket/USERS/",
        storage_options=storage_options
    )
    user_id = (
        df_users[df_users["username"] == username]["usuario_id"]
        .compute()
        .iloc[0]
    )

    df = df[df["usuario_id"] == user_id]

    df["mes"] = df["txn_date"].astype(str).str[:7]

    pdf = df.compute()

    total_entradas = pdf[pdf["tipo"] == "ENTRADA"]["monto"].sum()
    total_salidas = pdf[pdf["tipo"] == "SALIDA"]["monto"].sum()

    n_transacciones = len(pdf)

    ticket_promedio = pdf["monto"].mean()

    saldo_actual = pdf["saldo"].iloc[-1]

    resumen_mensual = pdf.groupby(["mes","tipo"])["monto"].sum().unstack(fill_value=0)

    promedio_entrada = resumen_mensual["ENTRADA"].mean()
    promedio_salida = resumen_mensual["SALIDA"].mean()

    transacciones_mensuales = pdf.groupby("mes").size().mean()

    mes_mayor_ingreso = resumen_mensual["ENTRADA"].idxmax()
    mes_mayor_gasto = resumen_mensual["SALIDA"].idxmax()

    transaccion_max = pdf["monto"].max()
    transaccion_min = pdf["monto"].min()

    ratio_ingreso_gasto = total_entradas / total_salidas if total_salidas != 0 else None

    saldo_promedio = pdf.groupby("mes")["saldo"].mean().mean()

    meses_activos = pdf["mes"].nunique()

    num_cuenta = OBTENER_CUENTA(username)

    return {
        "saldo_actual": saldo_actual,
        "total_entradas": total_entradas,
        "total_salidas": total_salidas,
        "n_transacciones": n_transacciones,
        "ticket_promedio": ticket_promedio,
        "promedio_entrada": promedio_entrada,
        "promedio_salida": promedio_salida,
        "transacciones_mensuales": transacciones_mensuales,
        "mes_mayor_ingreso": mes_mayor_ingreso,
        "mes_mayor_gasto": mes_mayor_gasto,
        "transaccion_max": transaccion_max,
        "transaccion_min": transaccion_min,
        "ratio_ingreso_gasto": ratio_ingreso_gasto,
        "saldo_promedio": saldo_promedio,
        "meses_activos": meses_activos,
        "num_cuenta": num_cuenta
    }