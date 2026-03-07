
# BancoBot 🤖🏦

BancoBot es un **asistente financiero automatizado por correo electrónico** que permite a los usuarios consultar información bancaria de forma segura mediante un sistema conversacional.

El bot procesa correos entrantes, autentica al usuario y permite solicitar diferentes servicios financieros como:

- 📄 Extractos bancarios
- 📑 Certificados bancarios
- 📊 Resumen general de cuenta

El sistema está diseñado como un **proyecto académico de ingeniería de datos**, integrando tecnologías modernas de procesamiento distribuido y almacenamiento tipo data lake.

---

# Arquitectura del Sistema

El proyecto utiliza una arquitectura basada en contenedores y procesamiento distribuido.

Usuario (Email)
│
▼
BancoBot (Python Email Bot)
│
▼
MinIO (Data Lake tipo S3)
│
▼
Dask (procesamiento distribuido)
│
▼
Generación de PDFs

Componentes principales:

| Componente | Función |
|---|---|
| Python Bot | Procesa correos y gestiona sesiones |
| MinIO | Almacenamiento tipo S3 |
| Dask | Procesamiento de datos en Parquet |
| Docker | Orquestación del sistema |
| ReportLab | Generación de PDFs |

---

# Funcionalidades

## 1️ Autenticación de usuarios

El bot solicita:

- Usuario
- Contraseña

Las contraseñas se validan mediante **hash SHA256**.

Después de autenticarse, el usuario accede al menú de servicios.

---

## 2️ Extracto Bancario

Permite generar un extracto de un mes específico.

Proceso:

1. Se consultan los meses disponibles del usuario
2. El usuario envía el mes en formato `YYYY-MM`
3. Se genera un **PDF con las transacciones**

Los datos se leen desde archivos **Parquet almacenados en MinIO** utilizando Dask.

---

## 3️ Certificado Bancario

Genera automáticamente un certificado bancario en PDF que incluye:

- Nombre del titular
- Número de cuenta
- Fecha de emisión
- Ciudad de emisión

El documento se genera automáticamente con **ReportLab**.

---

## 4️ Resumen Financiero

Genera un resumen analítico de la cuenta:

- saldo actual
- total de ingresos
- total de gastos
- promedio mensual
- mes con mayor ingreso
- mes con mayor gasto
- número de transacciones
- ratio ingreso/gasto

Este análisis se calcula usando **Dask + Pandas**.

---

# Generación de Datos

El sistema incluye un módulo de **simulación de transacciones bancarias** para crear grandes volúmenes de datos.

Características:

- hasta **90,000 transacciones por día**
- simulación de **90 días**
- almacenamiento en formato **Parquet**
- escritura directa a **MinIO (S3 compatible)**

Esto permite simular un entorno de **Big Data financiero**.

---

# Creación Automática de Usuarios

Después de generar transacciones, el sistema crea automáticamente usuarios bancarios.

Cada usuario incluye:

- usuario (`user{id}@banco.com`)
- contraseña (`pass{id}`)
- cuenta bancaria
- PIN

Los usuarios se almacenan en **MinIO en formato Parquet**.

---

# Infraestructura con Docker

El proyecto utiliza Docker para desplegar todos los servicios.

Servicios incluidos:

- **MinIO** → almacenamiento tipo S3
- **Aplicación Python** → bot + ETL

Configuración principal:

```yaml
services:
  minio:
    image: minio/minio

  app:
    build: .
    command: python PP.py
```

---

# Flujo de Inicialización

Cuando se inicia el sistema:

1️⃣ Espera a que MinIO esté disponible  
2️⃣ Crea el bucket del data lake  
3️⃣ Genera transacciones simuladas  
4️⃣ Genera usuarios  
5️⃣ Inicia el bot de correo  

Todo el flujo está automatizado en:

```
PP.py
```

---

# Instalación

## 1️ Clonar el repositorio

```bash
git clone https://github.com/tu_usuario/bancobot.git
cd bancobot
```

---

## 2️ Ejecutar con Docker

```bash
docker compose up --build
```

Esto iniciará:

- MinIO
- generación de datos
- creación de usuarios
- BancoBot

---

# Acceso a MinIO

Consola web:

```
http://localhost:9001
```

Credenciales dentro de los .py
---

# Estructura del Proyecto

```
.
├── chatico_bot.py
├── chatico_opt.py
├── etl_txn.py
├── etl_users.py
├── init_minio.py
├── PP.py
├── docker-compose.yml
├── Dockerfile
└── Images/
```

---

# Tecnologías Utilizadas

- Python
- Docker
- MinIO (S3)
- Dask
- Pandas
- ReportLab
- IMAP / SMTP
- Parquet

---

# Autores

Proyecto desarrollado por:

**Juan David Pérez Novoa**
**Camilo Briceño**  
Maestría en Analítica Aplicada  
Universidad de La Sabana


