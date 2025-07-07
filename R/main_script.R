
# Cargamos librerias necesarias. Se considera posible instalacion previa

packages <- c("data.table", "purrr", "furrr", "sf", "parallel", "arrow", "mapSpain", "future", "tidyverse")

installed <- packages %in% installed.packages()

if (any(!installed)) {
  install.packages(packages[!installed])
}

lapply(packages, library, character.only = TRUE)

# Ejercicio 4.2. BIG DATA

# 1. Cargamos librerias (se modifica script anterior con mapSpain y future)
library(sf)
library(mapSpain)
library(data.table)
library(purrr)
library(furrr)
library(arrow)
library(parallel)
library(future)
library(tidyverse)

# 2. Obtenemos el poligono de la provincia de Zaragoza
provincias <- esp_get_prov()                        # Descargamos mapa de todas las provincias
zaragoza <- provincias[provincias$cpro == "50", ]   # Seleccionamos Zaragoza
zaragoza <- st_transform(zaragoza, 25830)

# 3. Descargamos archivos de la url propuesta en el ejercicio
url <- "https://data-emf.creaf.cat/public/spatial_examples/exercise_4_2/" 
dias <- sprintf("202504%02d.gpkg", 1:10)          # Días que seleccionamos. Del 1 al 10, en este caso.

dest_folder <- "../raw_data"                           # Carpeta de destino

# Generamos un bucle para la descarga
for (dia in dias) {
  dest_file <- file.path(dest_folder, dia)
  if (!file.exists(dest_file)) {
    download.file(url = paste0(url, dia), destfile = dest_file)
    message(paste("Descargado:", dia))
  } else {
    message(paste("Ya existe:", dia))
  }
}

# 4. Preparamos los datos para su posterior procesamiento

# Convertimos el poligono a WKT, para poder filtrar en read_sf
zaragoza_wkt <- sf::st_as_text(sf::st_geometry(zaragoza))

# Obtenemos los nombres de las columnas, para poder seleccionar las que nos interesan: MeanTemperature, Precipitation &
# MeanRelativeHumidity
datos <- sf::read_sf("../raw_data/20250401.gpkg", layer = "20250401")
head(datos)

# Preparamos la lista de archivos para procesar

data_folder <- "../raw_data"
archivos <- list.files(data_folder, pattern = "\\.gpkg$", full.names = TRUE)

# 5. Configuramos paralelizacion
parallel::detectCores()

plan(multisession, workers = 5)

# 6. Generamos una funcion, para el procesado de los archivos
procesar_archivo <- function(archivo) {
  capa <- tools::file_path_sans_ext(basename(archivo))
  
  consulta <- sprintf('
    SELECT MeanTemperature, Precipitation, MeanRelativeHumidity, geom 
    FROM "%s"
  ', capa)
  
  datos <- tryCatch({
    sf::read_sf(archivo, query = consulta, wkt_filter = zaragoza_wkt)
  }, error = function(e) {
    message(paste("Error leyendo", archivo, ":", e$message))
    return(NULL)
  })
  
  # Si no hay datos, saltar ese archivo
  if (is.null(datos) || nrow(datos) == 0) {
    return(NULL)
  }
  
  resumen <- datos %>% 
    summarise(
      MeanTemperature = mean(MeanTemperature, na.rm = TRUE),
      Precipitation = mean(Precipitation, na.rm = TRUE),
      MeanRelativeHumidity = mean(MeanRelativeHumidity, na.rm = TRUE)
    )
  
  resumen$archivo <- basename(archivo)
  return(resumen)
}

# 7. Ejecutamos el proceso en paralelo y juntamos los resultados en un data.frame
resultados <- future_map_dfr(archivos, procesar_archivo, .options = furrr_options(seed = TRUE))

print(resultados)
plot(resultados)

# 8. Guardamos el resultado en un archivo .csv
write.csv(resultados, "../data/daily_averages_zaragoza_geom.csv", row.names = FALSE)
