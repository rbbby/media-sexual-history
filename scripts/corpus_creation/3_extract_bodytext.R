library(data.table)
library(dplyr)
library(randomForest)
library(parallel)

####################
# FUNCTIONS
####################


check_prediction <- function(data, threshold) {
  data <- data[which(data$pred_bodytext>= threshold),]
  return(data)
}



extract_bodytext <- function(base_path = '/home/r4/Documents/full_data_20191118/',
                          newspaper = 'AFTONBLADET', 
                          threshold = 0.5,
                          i = 1,
                          j = 1000,
                          save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/'){
  
  message('Read in data')
  # all labled files
  files <- list.files(paste0(base_path, newspaper,'/classified0/'))
  files <- files[i:j]
  
  #
  full_files <- paste0(base_path, newspaper,'/classified0/',files)
  
  dts <- lapply(full_files, function(f) readRDS(f))
  
  
  # remove file if has other structure
  if(length(which(unlist(lapply(dts, function(x) ncol(x)))==1))>0){
    dts <- dts[-which(unlist(lapply(dts, function(x) ncol(x)))==1)]
  }
  
  # Parallel -  feature extraction
  message('Predict and Extract bodytext')
  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  threshold <- threshold
  var_func_list <- c('check_prediction','threshold') 
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())

  system.time(data <- parLapply(cl = cl,
                                X = dts,
                                fun = function(x) try(check_prediction(data = x,
                                                                       threshold = threshold))))
  stopCluster(cl)
  
  
 # print('Saving Bodytext Data')
  message('Saving data')
  names(data) <- gsub(pattern = '.rds', replacement = '', x = files)
  
  lapply(names(data), function(df) saveRDS(data[[df]], file = paste0(save_path,newspaper,'/',df,'.rds')))
  
# return(data)
  
}



# AFTONBLADET
paper <- 'AFTONBLADET'
i <- NULL
for(minibatch in 1:55) {
  if(is.null(i)){
    i <- 1
  } else {
    i <- i + 500
  }
  
  j <- i + 500
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/,',paper,'classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/,',paper,'classified0/')))
    
    extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                     newspaper = paper,
                     threshold = 0.5,
                     i = i,
                     j = j,
                     save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
    
    
    break
    
  }
  
  extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                   newspaper = paper,
                   threshold = 0.5,
                   i = i,
                   j = j,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
  
  
  
}


# DAGENS NYHETER
paper <- 'DAGENS NYHETER'
i <- NULL
for(minibatch in 1:60) {
  if(is.null(i)){
    i <- 1
  } else {
    i <- i + 500
  }
  
  j <- i + 500
  
  
  if(j >= length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))
    
    extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                     newspaper = paper,
                     threshold = 0.5,
                     i = i,
                     j = j,
                     save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
    
    
    break
    
  }
  
  extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                   newspaper = paper,
                   threshold = 0.5,
                   i = i,
                   j = j,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
  
  
  
}






# EXPRESSEN
paper <- 'EXPRESSEN'
i <- NULL
for(minibatch in 1:60) {
  if(is.null(i)){
    i <- 1
  } else {
    i <- i + 500
  }
  
  j <- i + 500
  
  if(j >= length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))
    
    extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                     newspaper = paper,
                     threshold = 0.5,
                     i = i,
                     j = j,
                     save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
    
    
    break
    
  }
  
  extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                   newspaper = paper,
                   threshold = 0.5,
                   i = i,
                   j = j,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
  
  
  
}






# SVENSKA DAGBLADET
paper <- 'SVENSKA DAGBLADET'
i <- NULL
for(minibatch in 1:60) {
  if(is.null(i)){
    i <- 1
  } else {
    i <- i + 500
  }
  
  j <- i + 500
  
  if(j >= length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))
    
    extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                     newspaper = paper,
                     threshold = 0.5,
                     i = i,
                     j = j,
                     save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
    
    
    break
    
  }
  
  extract_bodytext(base_path = '/home/r4/Documents/full_data_20191118/',
                   newspaper = paper,
                   threshold = 0.5,
                   i = i,
                   j = j,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/') 
  
  
  
}






