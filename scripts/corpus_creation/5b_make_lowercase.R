library(data.table)
library(dplyr)
library(randomForest)
library(parallel)

####################
# FUNCTIONS
####################




# MAKE LOWER CASE - MAIN FUNCTION
final_preproc_file_lower <- function(data_path,
                                     newspaper,
                                     save_path,
                                     rare_words_limits = 10,
                                     stop_list = NULL) {
  
  # READ DATA
  message('Read data')
  # all labled files
  data <- fread(data_path,
                     encoding = 'UTF-8',
                     header = FALSE,
                     stringsAsFactors = FALSE) %>%
    rename(id = V1,
           date = V2,
           content = V3)
  
  
  
  data$content <- tolower(data$content)
  
  
  
  
  ## SAVE data
  
  write.table(data,file = paste0(save_path,newspaper,".txt"),sep="\t",row.names=FALSE,col.names = FALSE)
  gc()
  
  
  
}




################
#  CLEAN DATA
###############

# AFTONBLADET
paper <- 'AFTONBLADET'
final_preproc_file_lower(data_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/AFTONBLADET.txt',
                         newspaper = paper,
                         save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc4/')



# SVENSKA DAGBLADET
paper <- 'SVENSKA DAGBLADET'
final_preproc_file_lower(data_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/SVENSKA DAGBLADET.txt',
                         newspaper = paper,
                         save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc4/')


# EXPRESSEN
paper <- 'EXPRESSEN'
final_preproc_file_lower(data_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/EXPRESSEN.txt',
                         newspaper = paper,
                         save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc4/')




# EXPRESSEN
paper <- 'DAGENS NYHETER'
final_preproc_file_lower(data_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc3/DAGENS NYHETER.txt',
                         newspaper = paper,
                         save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc4/')








