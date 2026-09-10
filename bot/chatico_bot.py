import imaplib
import email
import smtplib
import time
import hashlib
import dask.dataframe as dd
from email.message import EmailMessage
from bot.chatico_opt import EXTRACT_BANC, OBTENER_CUENTA, generar_pdf_certificado, generar_pdf_extracto, RESUMEN_CUENTA
import s3fs

def limpiar_cache_s3():
    s3fs.S3FileSystem.clear_instance_cache()

# ================= CONFIGURACION =================

EMAIL = "bog.chat.ico@gmail.com"
PASSWORD = "############"

IMAP_SERVER = "imap.gmail.com"
SMTP_SERVER = "smtp.gmail.com"

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

SESIONES = {}

# ================= LOG =================

def log(msg):
    print(f"[BOT] {msg}")

# ================= FUNCIONES =================

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def conectar_imap():
    log("Conectando a IMAP...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL, PASSWORD)
    mail.select("inbox")
    log("IMAP conectado")
    return mail


def enviar_respuesta(destinatario, subject_original, reply_message_id, cuerpo_html, adjunto=None):

    log(f"Enviando respuesta a {destinatario}")

    msg = EmailMessage()
    msg["Subject"] = subject_original if subject_original else "BancoBot"
    msg["From"] = EMAIL
    msg["To"] = destinatario

    if reply_message_id:
        msg["In-Reply-To"] = reply_message_id
        msg["References"] = reply_message_id

    msg.add_alternative(cuerpo_html, subtype="html")
    with open("assets/LOGO_BANCOBOT.png", "rb") as img:
        msg.get_payload()[0].add_related(
            img.read(),
            maintype="image",
            subtype="png",
            cid="<logo_bancobot>"
        )

    # ===============================
    # Adjuntar archivo si existe
    # ===============================
    if adjunto:
        with open(adjunto, "rb") as f:
            data = f.read()

        msg.add_attachment(
            data,
            maintype="application",
            subtype="pdf",
            filename=adjunto
        )

    with smtplib.SMTP_SSL(SMTP_SERVER, 465) as smtp:
        smtp.login(EMAIL, PASSWORD)
        smtp.send_message(msg)

    log("Correo enviado ✔")

def obtener_texto(mensaje):
    texto = ""

    if mensaje.is_multipart():
        for parte in mensaje.walk():
            content_type = parte.get_content_type()
            payload = parte.get_payload(decode=True)

            if not payload:
                continue

            if content_type == "text/plain":
                texto = payload.decode(errors="ignore")
                break

            if content_type == "text/html":
                texto = payload.decode(errors="ignore")
    else:
        texto = mensaje.get_payload(decode=True).decode(errors="ignore")

    texto = texto.strip()
    lineas = [l.strip() for l in texto.splitlines() if l.strip()]

    if not lineas:
        return ""

    for linea in lineas:
        if not linea.startswith(">"):
            return linea.strip()

    return lineas[0].strip()

def limpiar_header(valor):
    if valor:
        return valor.replace("\r", "").replace("\n", "").strip()
    return ""


def obtener_reply_id(mensaje):
    reply_to = mensaje.get("In-Reply-To")
    if reply_to:
        return limpiar_header(reply_to)
    return limpiar_header(mensaje.get("Message-ID"))

# ================= DB =================

def CHECK_USER(username):
    log(f"Validando usuario: {username}")

    df = dd.read_parquet(
        "s3://realbucket/USERS/",
        storage_options=storage_options,
        engine="pyarrow"
    ).compute()

    return df[df.username == username].shape[0] > 0

def CHECK_PASSWORD(username, password):
    log(f"Validando contraseña usuario: {username}")

    df = dd.read_parquet(
        "s3://realbucket/USERS/",
        storage_options=storage_options,
        engine="pyarrow"
    ).compute()

    user = df[df.username == username]
    password_hash = hash_password(password)
    return user.iloc[0]["password_hash"] == password_hash


# ================= BOT =================
def plantilla_base(contenido_body):
    return f"""
    <html>
    <body style="margin:0; padding:0; font-family: Arial, Helvetica, sans-serif; background-color:#f4f6f9;">
    
    <table align="center" width="600" style="background:white; border-radius:8px; overflow:hidden; box-shadow:0px 4px 12px rgba(0,0,0,0.1);">
        
        <!-- HEADER -->
        <tr>
            <td style="padding:10px 20px 0px 20px; text-align:center;">
                
                <img src="cid:logo_bancobot" 
                     style="width:400px; max-width:100%; height:auto; display:block; margin:0 auto; border:none;">

            </td>
        </tr>

        <!-- BODY -->
        <tr>
            <td style="padding:30px; color:#333;">
                {contenido_body}
            </td>
        </tr>


        <!-- FOOTER -->
        <tr>
            <td style="background:#f1f1f1; padding:20px; font-size:12px; color:#555; text-align:center;">
                
                <b>Proyecto Académico</b><br>
                Maestría en Analítica Aplicada<br>
                Universidad de La Sabana<br><br>

                Desarrollado por:<br>
                <b>Juan David Pérez Novoa</b><br>
                <b>Camilo Briceño</b><br><br>

                <span style="color:#999;">
                Este es un sistema automatizado — RESPONDER DIRECTAMENTE LO SOLICITADO. SI DESEA CERRAR SESIÓN INGRESE: SALIR o EXIT.
                </span>

            </td>
        </tr>

    </table>

    </body>
    </html>
    """


def procesar_respuesta(mensaje, remitente, subject, reply_message_id):
    texto = obtener_texto(mensaje).strip()
    session_id = remitente.lower().strip()

    log(f"Mensaje de: {remitente}")
    log(f"Texto detectado: '{texto}'")

    if session_id not in SESIONES:
        log("Nueva sesión creada")
        SESIONES[session_id] = {"estado": "esperando_usuario"}
        log("Enviando bienvenida")

        contenido = """
        <h2 style="color:#003366;">Bienvenido 👋</h2>

        <p>
        Gracias por usar <b>BancoBot</b>, tu asistente financiero inteligente.
        </p>

        <p>
        Para comenzar el proceso de autenticación:
        </p>

        <div style="background:#eaf2fb; padding:15px; border-radius:6px; margin:20px 0;">
            <b>Paso 1:</b> Ingresa tu usuario (correo del banco)
        </div>

        <p style="color:#666;">
        Ejemplo: <i>user1234@banco.com</i>
        </p>
        """

        cuerpo = plantilla_base(contenido)
        enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
        return

    # ================= COMANDO GLOBAL CERRAR =================
    if texto.lower() in ["cerrar", "salir", "logout", "exit"]:
        if session_id in SESIONES:
            SESIONES.pop(session_id)

        contenido= """
        <html>
        <body style="font-family: Arial, Helvetica, sans-serif;">
            <h3>Sesión cerrada correctamente 🔒</h3>
            <p>Gracias por usar BancoBot.</p>
            <p>Si deseas iniciar nuevamente, solo envía cualquier mensaje.</p>
        </body>
        </html>
        """
        cuerpo = plantilla_base(contenido)
        enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
        return

    estado = SESIONES[session_id]["estado"]
    log(f"Estado actual: {estado}")

    # USER
    if estado == "esperando_usuario":
        if not CHECK_USER(texto):
            contenido = """
            <html>
            <body style="font-family: Arial, Helvetica, sans-serif;">
                <h3>El usuario ingresado no ha sido encontrado 🌐.</h3>
                <p>Gracias por usar BancoBot. Puede intentar nuevamente</p>
            </body>
            </html>
            """

            cuerpo = plantilla_base(contenido)
            enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
            return
        
        SESIONES[session_id]["username"] = texto
        SESIONES[session_id]["estado"] = "esperando_password"
        contenido = f"""
        <h2 style="color:#003366;">¡Gracias por tu respuesta, {texto}! 🟢</h2>

        <p>
        Siguiendo con el proceso de autenticación:
        </p>

        <div style="background:#eaf2fb; padding:15px; border-radius:6px; margin:20px 0;">
            <b>Paso 2:</b> Ingresa contraseña...
        </div>

        <p style="color:#666;">
        Ejemplo: <i>AeIoU*XYZ</i>
        </p>
        """

        cuerpo = plantilla_base(contenido)
        enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
        return

    # PASSWORD
    if estado == "esperando_password":
        username = SESIONES[session_id]["username"]

        if CHECK_PASSWORD(username, texto):
            SESIONES[session_id]["estado"] = "AUTHENTICATED"
            global contenido_menu
            contenido_menu = """
            <h2 style="color:#003366;">Autenticación exitosa 🟢</h2>

            <p>Selecciona una opción respondiendo con el número correspondiente:</p>

            <div style="background:#eaf2fb; padding:15px; border-radius:6px; margin:20px 0;">

            <b>1)</b> Extracto bancario de un mes específico<br>

            <b>2)</b> Certificado Bancario<br>

            <b>3)</b> Resumen general de cuenta<br><br>
            </div>
            """
            cuerpo = plantilla_base(contenido_menu)
            enviar_respuesta(remitente, subject, reply_message_id, cuerpo)

        else:
            intentos = SESIONES[session_id].get("intentos", 0) + 1
            SESIONES[session_id]["intentos"] = intentos

            if intentos >= 3:
                SESIONES.pop(session_id)
                contenido = "<p>Cuenta bloqueada por intentos fallidos.</p>"
            else:
                contenido = f"<p>Contraseña incorrecta. Intento {intentos}/3</p>"
        
            cuerpo = plantilla_base(contenido)
            enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
        return

# ================= MENU PRINCIPAL =================
    if estado == "AUTHENTICATED":
        if texto.strip() == "1":
            username = SESIONES[session_id]["username"]
            meses = EXTRACT_BANC(username, None, 1)

            contenido = f"""
            <html>
            <body>
                <h3>📄 Meses disponibles</h3>
                <p>{', '.join(meses)}</p>
                <p>Escribe el año-mes en formato <b>YYYY-MM</b></p>
            </body>
            </html>
            """

            SESIONES[session_id]["estado"] = "esperando_mes_OP1"

            cuerpo = plantilla_base(contenido)
            enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
            return
    
        elif texto.strip() == "2":
            username = SESIONES[session_id]["username"]

            cuenta = OBTENER_CUENTA(username)
            pdf_path = generar_pdf_certificado(username, cuenta)

            contenido = """
            <html>
            <body>
                <h3>📄 Certificado Bancario</h3>
                <p>Adjuntamos su certificado bancario.</p>
                <p>Gracias por usar <b>BancoBot</b>.</p>
            </body>
            </html>
            """
            cuerpo = plantilla_base(contenido)
            enviar_respuesta(remitente, subject, reply_message_id, cuerpo, pdf_path)
            enviar_respuesta(remitente, subject, reply_message_id, plantilla_base(contenido_menu))

        elif texto.strip() == "3":

            username = SESIONES[session_id]["username"]

            resumen = RESUMEN_CUENTA(username)

            contenido = f"""
            <h3>📊 Resumen general de cuenta {resumen["num_cuenta"]}</h3>

            <b>Saldo actual:</b> ${resumen["saldo_actual"]:,.0f}<br><br>

            <b>Total entradas:</b> ${resumen["total_entradas"]:,.0f}<br>
            <b>Total salidas:</b> ${resumen["total_salidas"]:,.0f}<br>
            <b>Total transacciones:</b> {resumen["n_transacciones"]}<br><br>

            <b>Promedio mensual entradas:</b> ${resumen["promedio_entrada"]:,.0f}<br>
            <b>Promedio mensual salidas:</b> ${resumen["promedio_salida"]:,.0f}<br>
            <b>Promedio mensual transacciones:</b> {resumen["transacciones_mensuales"]:.1f}<br><br>

            <b>Mes con mayor ingreso:</b> {resumen["mes_mayor_ingreso"]}<br>
            <b>Mes con mayor gasto:</b> {resumen["mes_mayor_gasto"]}<br><br>

            <b>Transacción máxima:</b> ${resumen["transaccion_max"]:,.0f}<br>
            <b>Transacción mínima:</b> ${resumen["transaccion_min"]:,.0f}<br><br>

            <b>Ratio ingreso/gasto:</b> {resumen["ratio_ingreso_gasto"]:.2f}<br>
            <b>Saldo promedio mensual:</b> ${resumen["saldo_promedio"]:,.0f}<br>
            <b>Meses activos:</b> {resumen["meses_activos"]}

            """
            cuerpo = plantilla_base(contenido)
            enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
            enviar_respuesta(remitente, subject, reply_message_id, plantilla_base(contenido_menu))

        else:
            contenido = """
            <html>
            <body>
                <p>SE HA SELECCIONADO UNA OPCIÓN NO VALIDA.</p>
            </body>
            </html>
            """

            cuerpo = plantilla_base(contenido)
            enviar_respuesta(remitente, subject, reply_message_id, cuerpo)
            enviar_respuesta(remitente, subject, reply_message_id, plantilla_base(contenido_menu))
            return


    # ================= ESPERANDO MES =================
    elif estado == "esperando_mes_OP1":

        username = SESIONES[session_id]["username"]
        mes = texto.strip()

        df_extracto = EXTRACT_BANC(username, mes, 2)
        df_extracto["monto"] = df_extracto["monto"].apply(
            lambda x: f"${x:,.0f}",
            meta=("monto", "str")
        )

        df_extracto["saldo"] = df_extracto["saldo"].apply(
            lambda x: f"${x:,.0f}",
            meta=("saldo", "str")
        )

        df_extracto["txn_date"] = df_extracto["txn_date"].dt.strftime("%Y-%m-%d")
        df_extracto.columns = df_extracto.columns.str.upper()
        pdf_path = generar_pdf_extracto(df_extracto)

        contenido = f"""
        <html>
        <body>
            <p>✅ Extracto generado para <b>{mes}</b></p>
        </body>
        </html>
        """

        cuerpo = plantilla_base(contenido)

        SESIONES[session_id]["estado"] = "AUTHENTICATED"

        enviar_respuesta(remitente, subject, reply_message_id, cuerpo, pdf_path)
        enviar_respuesta(remitente, subject, reply_message_id, plantilla_base(contenido_menu))
        return
            

# ================= LOOP PARA NOTEBOOK =================

def check_emails_once():
    log("Revisando correos...")

    mail = conectar_imap()
    status, mensajes = mail.search(None, "UNSEEN")

    ids = mensajes[0].split()

    log(f"Correos nuevos: {len(ids)}")

    for i in ids:
        status, datos = mail.fetch(i, "(RFC822)")
        mensaje = email.message_from_bytes(datos[0][1])

        remitente = email.utils.parseaddr(mensaje.get("From"))[1].lower().strip()
        subject = limpiar_header(mensaje.get("Subject"))
        reply_message_id = obtener_reply_id(mensaje)

        if remitente == EMAIL:
            continue

        procesar_respuesta(mensaje, remitente, subject, reply_message_id)

        mail.store(i, '+FLAGS', '\\Seen')

    mail.logout()
    log("Fin revisión")

if __name__ == "__main__":
    log("BancoBot iniciado")

    while True:
        try:
            limpiar_cache_s3()
            check_emails_once()
        except Exception as e:
            log(f"ERROR: {e}")

        time.sleep(10)
