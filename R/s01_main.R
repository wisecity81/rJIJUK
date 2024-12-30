#' DB2folder
#'
#' Split Jijuk DB by BJD, and save them in the folder made of BJD.
#' @param db data.frame
#' @param BJDcol BJD column name
#' @param dbSrc db source
#' @param jpth jijuk path
#' @return NULL
#' @export
DB2folder <- function(db, BJDcol, dbSrc, jpth = "R:/_library/_jijuk/") {
  # db: data.frame
  # BJDcol: BJD column name
  # jpth: jijuk path
  DBpth = paste0(jpth, "db")
  BJDlst = db[[BJDcol]] %>% unique
  for (bjd_i in BJDlst) {
    bjd_parts = strsplit(bjd_i, " ")[[1]]
    dbSubFolder = paste0(bjd_parts, collapse = "/")
    if (file.exists(file.path(DBpth,dbSubFolder)) == FALSE) {
      dir.create(file.path(DBpth, dbSubFolder), recursive = TRUE)
    }
    saveRDS(db[db[[BJDcol]] == bjd_i, ]
            , file = paste0(file.path(DBpth, dbSubFolder, dbSrc)
                            , ".rds"
                            )
            )
  }
}
