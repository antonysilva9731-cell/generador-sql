from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from generator import leer_archivo, generar_create_table, generar_insert, limpiar_nombre_tabla
from pathlib import Path
from fastapi.responses import RedirectResponse

import os

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")

# límite de archivo (5 MB)
MAX_FILE_SIZE = 5 * 1024 * 1024


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/generar")
async def generar_sql(
    request: Request,
    file: UploadFile = File(...),
    tabla: str = Form(...),
    db: str = Form(...),
    tipo_sql: str = Form(...)
):

    tabla = limpiar_nombre_tabla(tabla)

    contenido = await file.read()

    if len(contenido) > MAX_FILE_SIZE:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": "El archivo supera el tamaño máximo permitido (5 MB)."
            }
        )

    filename = Path(file.filename).name
    ruta_archivo = f"uploads/{filename}"

    with open(ruta_archivo, "wb") as buffer:
        buffer.write(contenido)

    df = leer_archivo(ruta_archivo)

    if tipo_sql == "create":
        sql_generado = generar_create_table(df, tabla)
    else:
        sql_generado = generar_insert(df, tabla)

    ruta_sql = f"uploads/{tabla}.sql"

    with open(ruta_sql, "w", encoding="utf-8") as f:
        f.write(sql_generado)

    os.remove(ruta_archivo)

    return templates.TemplateResponse(
        "resultado.html",
        {
            "request": request,
            "archivo_sql": f"{tabla}.sql"
        }
    )

from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

@app.get("/descargar/{archivo}")
async def descargar_sql(archivo: str):

    ruta = f"uploads/{archivo}"

    return FileResponse(
        ruta,
        media_type="application/sql",
        filename=archivo,
        background=BackgroundTask(os.remove, ruta)
    )

@app.get("/nuevo/{archivo}")
async def nuevo_archivo(archivo: str):

    ruta = f"uploads/{archivo}"

    if os.path.exists(ruta):
        os.remove(ruta)

    return RedirectResponse(url="/")

@app.get("/contact", response_class=HTMLResponse)
async def contact(request: Request):
    return templates.TemplateResponse("contact.html", {"request": request})


@app.get("/privacy", response_class=HTMLResponse)
async def privacy(request: Request):
    return templates.TemplateResponse("privacy.html", {"request": request})


@app.get("/terms", response_class=HTMLResponse)
async def terms(request: Request):
    return templates.TemplateResponse("terms.html", {"request": request})