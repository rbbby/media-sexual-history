library(data.table)
library(dplyr)
library(randomForest)
library(parallel)

####################
# FUNCTIONS
####################

select_cols <- function(data) {
  data %>% 
    select(id,date,content)
}



  
format_datafiles <- function(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                               newspaper = 'AFTONBLADET',
                             save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/'){
    
    message('Read in data')
    # all labled files
    files <- list.files(paste0(base_path, newspaper,'/'))
    
    #
    full_files <- paste0(base_path, newspaper,'/',files)
    
    dts <- lapply(full_files, function(f) readRDS(f))
    
    
    # remove file if has other structure
    if(length(which(unlist(lapply(dts, function(x) ncol(x)))==1))>0){
      dts <- dts[-which(unlist(lapply(dts, function(x) ncol(x)))==1)]
    }
    
    # Parallel -  feature extraction
    message('Select columns')
    
    cores <- 2
    gc()
    cl <- makeCluster(cores)
    var_func_list <- c('select_cols') 
    clusterEvalQ(cl, {library(dplyr)})
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(data <- parLapply(cl = cl,
                                  X = dts,
                                  fun = function(x) try(select_cols(data = x))))
    stopCluster(cl)
    gc()
    
    data <- data.table::rbindlist(data, fill = T)
    
    
    message('Save data with new format')
    write.table(data,file = paste0(save_path,newspaper,".txt"),sep="\t",row.names=FALSE,col.names = FALSE)
    
}



format_datafiles(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                 newspaper = 'AFTONBLADET',
                 save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/')



format_datafiles(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                 newspaper = 'EXPRESSEN',
                 save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/')


format_datafiles(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                 newspaper = 'DAGENS NYHETER',
                 save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/')




format_datafiles(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                 newspaper = 'SVENSKA DAGBLADET',
                 save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/')