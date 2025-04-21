import azure.functions as func
import logging
import pandas as pd
import io
import os
import urllib.request
from azure.storage.blob import BlobServiceClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

app = func.FunctionApp()

#inicio trigger
@app.function_name(name="http_trigger")
@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🟡 Iniciando función HTTP.")

    try:
        STORAGE_CONTAINER_NAME = "bronze"
        STORAGE_CONNECTION_STRING = os.environ["STORAGE_CONNECTION_STRING"]

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

        # === UBIGEO ===
        logging.info("🔹 Scraping de UBIGEO...")
        url_ubigeo = "https://www.datosabiertos.gob.pe/dataset/codigos-equivalentes-de-ubigeo-del-peru"
        driver.get(url_ubigeo)
        wait = WebDriverWait(driver, 20)
        link_ubigeo = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href, 'cloud.minsa.gob.pe') and contains(@href, 'download')]")
            )
        ).get_attribute("href")

        req_ubigeo = urllib.request.Request(link_ubigeo, headers={'User-Agent': 'Mozilla/5.0'})
        response_ubigeo = urllib.request.urlopen(req_ubigeo)
        df_ubigeo = pd.read_csv(response_ubigeo)
        buffer_ubigeo = io.StringIO()
        df_ubigeo.to_csv(buffer_ubigeo, index=False)
        buffer_ubigeo.seek(0)

        blob_ubigeo = blob_service_client.get_blob_client(container=STORAGE_CONTAINER_NAME, blob="TB_UBIGEOS.csv")
        blob_ubigeo.upload_blob(buffer_ubigeo.getvalue(), overwrite=True)

        # === DENUNCIAS ===
        logging.info("🔹 Scraping de DENUNCIAS...")
        url_denuncias = "https://observatorio.mininter.gob.pe/proyectos/base-de-datos-hechos-delictivos-basados-en-denuncias-en-el-sidpol"
        driver.get(url_denuncias)
        time.sleep(5)

        link_excel = driver.find_element(
            By.XPATH, "//a[contains(@href, 'Base%20de%20datos%20SIDPOL%20a%20febrero%20del%202025.xlsx')]"
        ).get_attribute("href")

        req_excel = urllib.request.Request(link_excel, headers={'User-Agent': 'Mozilla/5.0'})
        response_excel = urllib.request.urlopen(req_excel)
        excel_bytes = response_excel.read()

        excel_file = io.BytesIO(excel_bytes)
        sheets_dict = pd.read_excel(excel_file, sheet_name=None)
        last_two_sheets = list(sheets_dict.keys())[-2:]

        for i, sheet_name in enumerate(last_two_sheets, start=1):
            df = sheets_dict[sheet_name]
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)

            blob_client = blob_service_client.get_blob_client(
                container=STORAGE_CONTAINER_NAME,
                blob=f"TB_DENUNCIAS_{i}.csv"
            )
            blob_client.upload_blob(buffer.getvalue(), overwrite=True)

        driver.quit()
        return func.HttpResponse("✅ Archivos subidos correctamente.", status_code=200)

    except Exception as e:
        logging.error(f"❌ Error durante el proceso: {e}")
        return func.HttpResponse(f"❌ Error: {str(e)}", status_code=500)
