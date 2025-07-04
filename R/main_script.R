
# Cargamos librerias necesarias. Se considera posible instalacion previa

packages <- c("data.table", "purrr", "furrr", "sf", "parallel", "arrow")

installed <- packages %in% installed.packages()

if (any(!installed)) {
  install.packages(packages[!installed])
}

lapply(packages, library, character.only = TRUE)