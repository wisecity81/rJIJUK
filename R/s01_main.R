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
  # if nrow(db) == 0, return "db is empty"

  if (nrow(db) == 0) {
    return("db is empty")
  } else {
    library(progress)

    DBpth = paste0(jpth, "db")
    BJDlst = db[[BJDcol]] %>% unique

    # 진행 바 생성
    pb <- progress_bar$new(
      format = "  진행중 [:bar] :current/:total (:elapsed)",
      total = length(BJDlst),    # 총 작업 개수
      clear = FALSE,  # 완료 후 진행 바 유지
      width = 60      # 진행 바 너비
    )
    cat(.now(), "총 ", length(BJDlst), "개의 BJD 처리\n")
    for (i in 1:length(BJDlst)) {
      bjd_i = BJDlst[i]
      cat(.now(), "Splitting ", bjd_i, "(",i,"/",length(BJDlst), ")\n", file = ".log", append = T)
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
      pb$tick()      # 진행 바 업데이트
    }
  }
}

#' Dwn2standBy
#'
#' This function is to filter the files in the download folder by the time and DB type, and unzip the files to the stand_by folder.
#' If target files are already exist in the stand_by folder, skip them.
#' If pub_time is specified, the function will download the files that file name contains the pub_time, instead of the time range.
#' Log the process of the function to .log file.
#' @param DB DB type
#' @param .last If TRUE, the function will download the files that are created after the last download.
#' @param pub_time publication time
#' @param time time range
#' @param download download folder
#' @param stand_by stand_by folder
#' @return NULL
#' @export
Dwn2standBy = function(DB, .last = F, pub_time = NULL, time = c("1900-1-1","9999-12-31"), download = "./_download(zip)", stand_by = "./_stand_by") {
  # DB: DB type
  # .last: If TRUE, the function will download the files that are created after the last download.
  # pub_time: publication time
  # time: time range
  # download: download folder
  # stand_by: stand_by folder
  # return: NULL

  library(progress)

  fLst = list.files(download, full.names = T)
  fLstDF = file.info(fLst)
  lastTm = fLstDF$ctime %>% as.Date() %>% unique() %>% max()
  if (.last) {
    time = c(paste(lastTm, "00:00:01"), paste(lastTm, "23:59:59")) %>% as.POSIXct()
  }
  target = .s_dtt(fLst, paste0("_", DB, "_"))
  if (!is.null(pub_time)) {
    target = target & .s_dtt(fLst, paste0("_", pub_time))
    fLst = fLst[target]
  } else {
    target = target & fLstDF$ctime >= as.POSIXct(time[1]) & fLstDF$ctime <= as.POSIXct(time[2])
    fLst = fLst[target]
  }

  if (length(fLst) == 0) {
    return(invisible())
  }
  # 진행 바 생성
  pb <- progress_bar$new(
    format = "  진행중 [:bar] :current/:total (:elapsed)",
    total = length(fLst),    # 총 작업 개수
    clear = FALSE,  # 완료 후 진행 바 유지
    width = 60      # 진행 바 너비
  )
  cat(.now(), "총 ", length(fLst), "파일 처리\n")
  for (i in 1:length(fLst)) {
    suppressWarnings(unzip(zipfile = fLst[i], exdir = stand_by, overwrite = F))
    cat(.now(), "Extracted ", fLst[i], "(", i, "/", length(fLst), ")\n", file = ".log", append = T)
    pb$tick()      # 진행 바 업데이트
  }
  return(invisible())
}

#' downloadInfo
#'
#' This function is to list files in download folder.
#' @param download download folder
#' @return data.frame
#' @export
downloadInfo = function(download = "./_download(zip)") {
  # download: download folder
  # return: NULL

  fLst = list.files(download, full.names = T)
  fLstDF = as.data.frame(fLst) %>% bind_cols(file.info(fLst))

  return(fLstDF)
}

#' stnBy2DB
#'
#' This function is to list the files in the stand_by folder, and is to split the files into DBs by BJD.
#' And then splited DBs are saved in the DB folder.
#' @param stand_by stand_by folder
#' @return NULL
stnBy2DB = function(stand_by = "./_stand_by") {

  DBs = list("D157" = "V3") # BJD column name
  fLst = list.files(stand_by)
  # Extract db tpye from the file name
  db_i = fLst %>% .s_xtr("^(AL)_D[0-9]+") %>% .s_rm("^(AL)_")

  # Define sep. character for each DB.
  sep_i = c("D157" = ",")

  # Create a progress bar
  pb <- progress_bar$new(
    format = "  진행중 :current/:total (:elapsed 초 경과)",
    total = length(fLst),    # 총 작업 개수
    clear = FALSE,  # 완료 후 진행 바 유지
    width = 60      # 진행 바 너비
  )

  # Loop for each file
  for (i in 1:length(fLst)) {
    fn = fLst[i]
    db = db_i[i]
    bjdcol = DBs[[db]]

    # Read the file
    cat(.now(), "Split", fn, "\n", file = ".log", append = TRUE)
    cat(.now(), "Split", fn, "\n")
    a = .rdSmart_csv(paste0(stand_by, "/", fn), .sep = sep_i[db]) %>% .noMsg()

    # split and save the data by BJD
    DB2folder(db = a, BJDcol = bjdcol, dbSrc = fn)

    # Update the progress bar
    pb$tick() # Update the progress bar
    cat("\n")
  }
}

#' cleanUpNonBJD
#'
#' Find all rds files in ./db and read them into data frame.
#' source name is that found file name without extension ".rds".
#' Loop DB2folder function for all rds files.
#' Rename the rds files to rds.done.
#' @param bjdFn BJD file name
#' @return NULL
#' @export
cleanUpNonBJD = function(bjdFn = "./_BJD/BJD.txt") {
  # Find all rds files in ./db and read them into data frame.
  # source name is that found file name without extension ".rds".
  # Loop DB2folder function for all rds files,
  # with option:
  #  db = data frame
  #  BJDcol = "V3"
  #  dbSrc = source name
  # And then rename extension of the rds file from ".rds"  to ".rds.done".
  fileLst = list.files("./db", pattern = ".rds", full.names = TRUE)
  sourceLst = gsub(".rds", "", basename(fileLst))
  bjd = .rdSmart_csv(fn = bjdFn , .sep = "\t") %>% mutate(code = V1, bjd = V2, exist = V3) %>% select(code, bjd, exist)
  for (i in 1:length(fileLst)) {
    cat(fileLst[i], "\n")
    DB2folder(
      db = fileLst[i] %>% readRDS() %>% merge(bjd, by.x = "V2", by.y = "code") %>% mutate(V3 = bjd) %>% select(-exist, -bjd),
      BJDcol = "V3",
      dbSrc = sourceLst[i]
    )
    file.rename(fileLst[i], gsub(".rds", ".rds.done", fileLst[i]))
  }
}
