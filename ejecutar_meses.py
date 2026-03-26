import subprocess
import calendar
import sys # Importamos sys para detectar tu entorno actual
import os

# Detectamos automáticamente el ejecutable de Python de tu entorno virtual activo
python_executable = sys.executable 

años = [2025]
#del 1 al 7 y luego del 7 al 13
for año in años:
    for mes in range(7, 13):
        ultimo_dia = calendar.monthrange(año, mes)[1]
        start_date = f"{año}-{mes:02d}-01"
        end_date = f"{año}-{mes:02d}-{ultimo_dia:02d}"
        
        print(f"\n>>> PROCESANDO: {start_date} hasta {end_date}")
        
        # USAMOS python_executable en lugar de solo "python"
        comando = [
            python_executable, "-m", "pipeline.run",
            "--start", start_date,
            "--end", end_date
        ]
        
        try:
            subprocess.run(comando, check=True)
            print(f"✔ Mes {mes}/{año} completado.")
        except subprocess.CalledProcessError:
            print(f"❌ Error en {start_date}. Deteniendo.")
            sys.exit(1)