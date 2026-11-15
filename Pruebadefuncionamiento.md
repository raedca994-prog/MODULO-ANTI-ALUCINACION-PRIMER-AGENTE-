def process(self, input_data, nombre_archivo="memoria"):
    huella_entrada = calcular_huella(str(input_data)[:2000])
    datos, error = self._ingesta_segura(input_data)
    if error:
        return Result(False, None, huella_entrada, "", [error], nombre_archivo)
    datos_procesados = self._transformar(datos)
    ok, huella_salida = self.monitor.validar(datos_procesados)
    if not ok:
        return Result(False, None, huella_entrada, "", ["FALLO DE INTEGRIDAD"], nombre_archivo)
    return Result(True, datos_procesados, huella_entrada, huella_salida, [], nombre_archivo)

def _ingesta_segura(self, data):
    try:
        if isinstance(data, str) and data.endswith(".csv"):
            with open(data, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                filas = []
                for i, fila in enumerate(reader):
                    if i >= 10000:
                        break
                    filas.append(fila)
                return filas, None
        return data, None
    except FileNotFoundError:
        return None, "ERROR: Archivo no encontrado"
    except Exception as e:
        return None, f"ERROR: {str(e)}"

def _transformar(self, data):
    if isinstance(data, list):
        return {
            "total_registros": len(data),
            "columnas": list(data[0].keys()) if data else [],
            "muestra_verificada": data[:3],
        }
    return data

def procesar_lote(self, lista_archivos):
    resultados = []
    print(f"AGENTE {self.id} INICIANDO LOTE - Archivos: {len(lista_archivos)}")
    for archivo in lista_archivos:
        print(f"--> Procesando: {archivo} ...")
        res = self.process(archivo, nombre_archivo=archivo)
        if res.ok:
            print(f"    OK | Registros: {res.data['total_registros']} | Huella: {res.huella_salida[:16]}...")
        else:
            print(f"    FALLO | Error: {res.errores}")
        resultados.append(res)
    print(f"LOTE TERMINADO: {len(resultados)} archivos auditados")
    return resultados
