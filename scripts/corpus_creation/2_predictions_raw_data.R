library(data.table)
library(dplyr)
library(randomForest)
library(parallel)
library(xgboost)

# function for creating features
feature_extraction <- function(data){
  
  gc()
  print(unique(data$title))


  ymiddle <- max(data$y1)/2 * -1
  xmiddle <- max(data$x1)/2

  data %>%
    mutate(x2 = x1 + width, # find x2-coordinate
           y1 = y1 * -1 ,  # change sign of y1-coordinate
           y2 = y1 - height,  # find y2-coordinate
           area = height*width,  # find area size
           year = lubridate::year(as.Date(gsub(x = title, pattern = '[A-Z]', replacement = '')))) %>%  # find year
    mutate(n_characters = stringi::stri_count(content, regex = '[a-öA-Ö]'),  # find number of characters in content 
           n_capital_chars = stringi::stri_count(content, regex = '[[:upper:]]'), # find number of capital characters in content 
           n_numbers = stringi::stri_count(content, regex = '[0-9]')) %>%  # find number of numbers in content 
    rowwise() %>%
    mutate(n_words = length(stringr::str_split(string = content, pattern = ' ')[[1]]), # find number of words in content 
           placement = ifelse(x1 <= xmiddle & y1 <= ymiddle, 'upper_left','unknown'), # find placement of textblock (divide page in 4 parts), based on x1 & y1
           placement = ifelse(x1 >= xmiddle & y1 <= ymiddle, 'upper_right',placement), 
           placement = ifelse(x1 <= xmiddle & y1 >= ymiddle, 'lower_left',placement),
           placement = ifelse(x1 >= xmiddle & y1 >= ymiddle,'lower_right',placement),
           placement_p4 = ifelse(x2 <= xmiddle & y2 <= ymiddle, 'upper_left','unknown'), # find placement of textblock (divide page in 4 parts), based on x2 & y2
           placement_p4 = ifelse(x2 > xmiddle & y2 <= ymiddle, 'upper_right',placement_p4),
           placement_p4 = ifelse(x2 <= xmiddle & y2 > ymiddle, 'lower_left',placement_p4),
           placement_p4 = ifelse(x2 > xmiddle & y2 > ymiddle,'lower_right',placement_p4),
           
           paper = ifelse(stringr::str_detect(string = title, pattern = 'AFTONBLADET'), 'afb', # find paper
                          ifelse(stringr::str_detect(string = title, pattern = 'DAGENS NYHETER'),'dn',
                                 ifelse(stringr::str_detect(string = title, pattern = 'EXPRESSEN'),'exp','svd'))),
           paper = as.factor(paper)) %>%
    ungroup() -> d 

  gc()
  d$font_larger_12 <- ifelse(unlist(lapply(d$font_size, function(x) sum(ifelse(as.numeric(stringr::str_extract_all(x, pattern = '[:digit:]{2}')[[1]])>12,1,0))))!=0,1,0)    # does the textblock include text with larger size than 12  
  d$font_larger_20 <- ifelse(unlist(lapply(d$font_size, function(x) sum(ifelse(as.numeric(stringr::str_extract_all(x, pattern = '[:digit:]{2}')[[1]])>20,1,0))))!=0,1,0)    # does the textblock include text with larger size than 20
  d$bold <- ifelse(stringr::str_detect(tolower(d$font_style), pattern = 'bold'),1,0) # does the textblock include text with bold font
  d$italic <- ifelse(stringr::str_detect(tolower(d$font_style), pattern = 'italic'),1,0) # does the textblock include text with italic font
  d$underline <- ifelse(stringr::str_detect(tolower(d$font_style), pattern = 'underline'),1,0) # does the textblock include text with underlined font
  d$sansserif <- ifelse(stringr::str_detect(tolower(d$font_type), pattern = 'sans-serif'),1,0) # does the textblock include sans-serif text 

  # find page number
  id_page <- stringr::str_match(d$pic_name, "_(.*).jp2")[,2]
  d$id_page <- as.numeric(substring(id_page, first = nchar(id_page)-3, last = nchar(id_page)))
   
  # by page, calulate number of obseravtions =  textblocks on page 
  d %>%
    group_by(id_page) %>%
    summarise(n = n()) -> df_textblocks

  d$textblocks_on_page <- df_textblocks$n[match(d$id_page,df_textblocks$id_page)]
    
  # find part number
  id_part <- stringr::str_match(d$id, "#(.*)-")[,2]
  d$part <- as.numeric(gsub("-.*","",id_part))
  
  # is the page among the first 5 pages?
  d$first5page <- ifelse(d$part == 1 & d$id_page <= 5,1,0)
  
  gc()
  
  # EXTRACT LEXICAL FEATURES --
  
  # commercial
  d$price_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('rea$', ignore_case = T)))>0,1,0)))
  d$price_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('rabatt$', ignore_case = T)))>0,1, d$price_words)))
  d$price_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('\\:\\-', ignore_case = T)))>0,1, d$price_words)))
  d$price_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('kr', ignore_case = T)))>0,1, d$price_words)))
  
  # special characters
  spec_char <-  c(',', '.', ';', '?', '/', '\\', '`', '[', ']', '"', ':', '>', '<', '|', '-', '_', '=', '+', '(', ')', '^', '{', '}', '~', '\'', '*', '&', '%', '$', '!', '@', '#', '•','”','—','«','■','»')
  d$spec_char <- unlist(lapply(d$content, function(ss) sum(stringr::str_count(ss , pattern =  stringr::fixed(spec_char)))))
  
  
  # entertainment
  d$entertainment_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('PREMIÄR', ignore_case = F)))>0,1,0)))
  d$entertainment_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('biljetter', ignore_case = T)))>0,1, d$entertainment_words)))
  
  d$tv_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('TV', ignore_case = F)))>0,1,0)))
  
  # full ad page
  d$payed_page <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(ss, pattern =  stringr::regex('hela sidan är betalad', ignore_case = T)))>0,1,0)))
  d$payed_page <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(ss, pattern =  stringr::regex('hela sidan är en annons', ignore_case = T)))>0,1,d$payed_page)))
  d$payed_page <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(ss, pattern =  stringr::regex('är en annons från', ignore_case = T)))>0,1,d$payed_page)))
  d$payed_page <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(ss, pattern =  stringr::regex('hela denna tematidning är en annons från', ignore_case = T)))>0,1,d$payed_page)))
  d$payed_page <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(ss, pattern =  stringr::regex('hela denna bilaga är en annons från', ignore_case = T)))>0,1,d$payed_page)))
  
  
  gc()
  # section titles
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('sport', ignore_case = T)))>0,1,0)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('salu', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('nöje', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('börs', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('ekonomi', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('scen', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('stan', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('marknad', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('nyheter', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('kultur', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('TV', ignore_case = T)))>0,1,d$section_words)))
  d$section_words <- unlist(lapply(d$content, function(ss) ifelse(sum(stringr::str_detect(strsplit(ss , ' ')[[1]], pattern =  stringr::regex('radio', ignore_case = T)))>0,1,d$section_words)))
  
  
  
  # closest y-dist, up
  d$y_dist_up <- 0
  for(i in 1:nrow(d)){
    neg_inds <- which(c(d$y1[i] - d$y2[-i])<0)
    if(length(neg_inds)>0){
      d$y_dist_up[i] <- max(c(d$y1[i] - d$y2[-i])[neg_inds])
    }
    neg_inds <- NULL
  }
  
  # closest x-dist, left
  d$x_dist_left <- 0
  for(i in 1:nrow(d)){
    pos_inds <- which(c(d$x1[i] - d$x2[-i])>0)
    if(length(pos_inds)>0){
      d$x_dist_left[i] <- min(c(d$x1[i] - d$x2[-i])[pos_inds])
    }
    pos_inds <- NULL
  }  
  
  # closest x-dist, rights
  d$x_dist_right <- 0
  for(i in 1:nrow(d)){
    neg_inds <- which(c(d$x1[i] - d$x2[-i])<0)
    if(length(neg_inds)>0){
      d$x_dist_right[i] <- max(c(d$x1[i] - d$x2[-i])[neg_inds])
    }
    pos_inds <- NULL
  }  
  

  d %>% mutate(x1 = as.numeric(x1),
               y1 =  as.numeric(y1),
               area = as.numeric(area),
               n_characters = as.numeric(n_characters),
               id_page = as.numeric(id_page),
               textblocks_on_page = as.numeric(textblocks_on_page),
               part = as.numeric(part),
               n_numbers = as.numeric(n_numbers),
               n_characters = as.numeric(n_characters),
               n_words = as.numeric(n_words),
               words_area_ratio = n_words/area,
               number_character_ratio = n_numbers/n_characters,
               number_character_ratio = ifelse(number_character_ratio==Inf, 10, number_character_ratio),
               capital_character_ratio = n_capital_chars/n_characters,
               capital_character_ratio = ifelse(capital_character_ratio==Inf, 0, capital_character_ratio),
               words_area_ratio = ifelse(words_area_ratio==Inf, 0, words_area_ratio),
               placement = as.factor(placement),
               spec_char = as.numeric(spec_char)) -> d
  
  # replace more NA
  d %>%
    tidyr::replace_na(list(capital_character_ratio = 0,
                           number_character_ratio = 0,
                           font_larger_12 = 0,
                           font_larger_20 = 0,
                           bold = 0,
                           italic = 0,
                           number_character_ratio = 0,
                           capital_character_ratio = 0,
                           underline = 0,
                           sansserif = 0,
                           spec_char = 0)) -> d
  
  
  # some data fix
  d$topy1 <- ifelse(d$y1> -300, 1,0)
  d$numbers10 <- ifelse(d$n_numbers>10,1,0)
  
  # change factor-levels
  d %>%
    mutate(paper =  factor(paper, levels=c('afb','exp','dn','svd')),
           placement = factor(placement, levels=c('lower_left', 'lower_right', 'upper_left', 'upper_right'))) -> d
  

  return(d)
  
}





###################################################
###      CLASSIFY ALL DATA - SEMIPARALLELL      ###                     
###################################################

### GET SAVED MODEL



classify_data <- function(base_path = '/home/r4/Documents/full_data_20191118/',
                          newspaper = 'AFTONBLADET',
                          i = 1,
                          j = 1000) {
  
  message('Read data')
  # all labled files
  files <- list.files(paste0(base_path, newspaper, '/raw0/'))
  files <- files[i:j]
  full_files <- paste0(base_path, newspaper,'/raw0/',files)
  dts <- lapply(full_files, function(f) read.csv(f, stringsAsFactors = FALSE))
  
  
  # remove file if has other structure
  if(length(which(unlist(lapply(dts, function(x) ncol(x)))==1))>0){
    dts <- dts[-which(unlist(lapply(dts, function(x) ncol(x)))==1)]
  }
  
  # Parallel -  feature extraction
  message('Feature extraction')
  
  cores <- 12
  gc()
  cl <- makeCluster(cores)
  var_func_list <- c('feature_extraction','data.table','stringi','rbindlist','Encoding','iconv','stringr','tidyr','dplyr') 
  clusterExport(cl = cl, list(var_func_list))
  clusterEvalQ(cl, {library(data.table); library(stringi); library(stringr); library(dplyr)})
  
  system.time(data <- parLapply(cl = cl,
                                X = dts,
                                fun = function(x) try(feature_extraction(data = x))))
  stopCluster(cl)
  
  
  message('Classifying editorial')
  
  
  # EDITORIAL
  cores <- 12
  gc()
  cl <- makeCluster(cores)
  var_func_list <- c('model_editorial') 
  clusterEvalQ(cl, {library(data.table);  library(randomForest); library(xgboost)})
  clusterExport(cl = cl, list(var_func_list))

  
  system.time(pred_dataE <- parLapply(cl = cl,
                                     X = data,
                                     fun = function(x) try(predict(object = model_editorial,
                                                                   newdata = data.matrix(x[,model_editorial$feature_names])))))

  stopCluster(cl)

  
  # BODYTEXT
  message('Classifying bodytext')
  
  cores <- 12
  gc()
  cl <- makeCluster(cores)
  var_func_list <- c('model_bodytext') 
  clusterEvalQ(cl, {library(data.table);  library(randomForest); library(xgboost)})
  clusterExport(cl = cl, list(var_func_list))
  
  system.time(pred_dataB <- parLapply(cl = cl,
                                      X = data,
                                      fun = function(x) try(predict(object = model_bodytext,
                                                                    newdata = data.matrix(x[,model_bodytext$feature_names])))))
  
  stopCluster(cl)
  data <- Map(cbind, data, pred_editorial = pred_dataE)
  rm(pred_dataE)
  data <- Map(cbind, data, pred_bodytext = pred_dataB)
  rm(pred_dataB)
  gc()
  
  
  message('Saving data')
  names(data) <- gsub(pattern = '.csv', replacement = '', x = files)
  
  lapply(names(data), function(df) saveRDS(data[[df]], file = paste0(base_path,newspaper,'/classified0/',df,'.rds')))
  

}

model_editorial <- readRDS('/media/r3/Flyttflytt/models/xgb_editorial_10.rds')
model_bodytext <- readRDS('/media/r3/Flyttflytt/models/xgb_bodytext_10.rds')


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
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/raw0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/raw0/')))
    
    classify_data(base_path = '/home/r4/Documents/full_data_20191118/',
                  newspaper = paper,
                  i = i,
                  j = j)
    break
    
  }
  
  classify_data(base_path = '/home/r4/Documents/full_data_20191118/',
                newspaper = paper,
                i = i,
                j = j)

}



# DAGENS NYHETER

paper <- 'DAGENS NYHETER'
i <- NULL
for(minibatch in 1:55) {
  if(is.null(i)){
    i <- 1
  } else {
    i <- i + 500
  }
  
  j <- i + 500
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/raw0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/raw0/')))
    
    classify_data(base_path = '/home/r4/Documents/full_data_20191118/',
                  newspaper = paper,
                  i = i,
                  j = j)
    break
    
  }
  
  classify_data(base_path = '/home/r4/Documents/full_data_20191118/',
                newspaper = paper,
                i = i,
                j = j)
  
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
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/raw0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/raw0/')))
    
    classify_data(base_path = '/home/r4/Documents/full_data_20191118/',
                  newspaper = paper,
                  i = i,
                  j = j)
    break
    
  }
  
  classify_data(base_path = '/home/r4/Documents/full_data_20191118/',
                newspaper = paper,
                i = i,
                j = j)
  
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
  
  if(j > length(list.files(paste0('/home/r3/Documents/full_data_20191118/',paper,'/raw0/')))) {
    j <- length(list.files(paste0('/home/r3/Documents/full_data_20191118/',paper,'/raw0/')))
    
    classify_data(base_path = '/home/r3/Documents/full_data_20191118/',
                  newspaper = paper,
                  i = i,
                  j = j)
    break
    
  }
  
  classify_data(base_path = '/home/r3/Documents/full_data_20191118/',
                newspaper = paper,
                i = i,
                j = j)
  
}
