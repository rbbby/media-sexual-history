


#---------------#
# PART 1        #
#---------------#


#---------------------------
# AFTONBLADET
#---------------------------

library('dplyr')
library(data.table)
data_path <- '/data/data_media_group_threat/bodytext_preproc4/AFTONBLADET.txt'

# READ DATA
message('Read data')
# all labled files
data <- fread(data_path,
              header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)

data <- data[which(!is.na(stringr::str_match(data$id, "#1-(.*)")[,2])),]

data$content <- gsub("[^[:alnum:][:blank:]_]", "", data$content)

data <- data.table(data)
tok_data <- data[, strsplit(x = content, split = ' ', perl = T), by = c('id','date')]
names(tok_data) <- c('id','date','token')


vocab <- unique(tok_data$token)

spec_char <- vocab[which(vocab%like%'_')]
spec_char_first <- spec_char[which(substring(spec_char,first = 1,last=1)=='_')]
spec_char_last_ind <- unlist(lapply(spec_char, function(x) substring(x,first = nchar(x),last=nchar(x))))
spec_char_last <- spec_char[which(spec_char_last_ind=='_')]

if(length(spec_char_first)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_first),]
}

if(length(spec_char_last)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_last),]
}

# remove empty rows 
empty_ind <- which(tok_data$token=='')
if(length(empty_ind)!=0){
  tok_data <- tok_data[-empty_ind,]
}

# remove NA-tokens
na_ind <- which(is.na(tok_data$token))
if(length(na_ind)!=0){
  tok_data <- tok_data[-na_ind,]
}

#tok_data %>% arrange(count)

data <- tok_data[, text := paste0(token, collapse = ' '), by = c('id','date')]
data[, token := NULL]
data[, count := NULL]
data <- unique(data, by = 'id')


write.table(data,file = paste0("/home/r4/Documents/data_media_group_threat/bodytext_preproc5/AFTONBLADET_part1.txt"),sep="\t",row.names=FALSE,col.names = FALSE)


#---------------------------
# EXPRESSEN
#---------------------------

# EDITORIALS
library('dplyr')
library(data.table)
data_path <- '/data/data_media_group_threat/bodytext_preproc4/EXPRESSEN.txt'

# READ DATA
message('Read data')
# all labled files
data <- fread(data_path,
              header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)

data <- data[which(!is.na(stringr::str_match(data$id, "#1-(.*)")[,2])),]

data$content <- gsub("[^[:alnum:][:blank:]_]", "", data$content)


library(data.table)
data <- data.table(data)
tok_data <- data[, strsplit(x = content, split = ' ', perl = T), by = c('id','date')]
names(tok_data) <- c('id','date','token')


vocab <- unique(tok_data$token)

spec_char <- vocab[which(vocab%like%'_')]
spec_char_first <- spec_char[which(substring(spec_char,first = 1,last=1)=='_')]
spec_char_last_ind <- unlist(lapply(spec_char, function(x) substring(x,first = nchar(x),last=nchar(x))))
spec_char_last <- spec_char[which(spec_char_last_ind=='_')]

if(length(spec_char_first)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_first),]
}

if(length(spec_char_last)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_last),]
}

# remove empty rows 
empty_ind <- which(tok_data$token=='')
if(length(empty_ind)!=0){
  tok_data <- tok_data[-empty_ind,]
}

# remove NA-tokens
na_ind <- which(is.na(tok_data$token))
if(length(na_ind)!=0){
  tok_data <- tok_data[-na_ind,]
}

data <- tok_data[, text := paste0(token, collapse = ' '), by = c('id','date')]
data[, token := NULL]
data <- unique(data, by = 'id')


write.table(data,file = paste0("/home/r4/Documents/data_media_group_threat/bodytext_preproc5/EXPRESSEN_part1.txt"),sep="\t",row.names=FALSE,col.names = FALSE)



#---------------------------
# DAGENS NYHETER
#---------------------------
library('dplyr')
data_path <- '/data/data_media_group_threat/bodytext_preproc4/DAGENS NYHETER.txt'

# READ DATA
message('Read data')
# all labled files
data <- read.table(data_path,
                   encoding = 'UTF-8',
                   header = FALSE,
                   stringsAsFactors = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)

data <- data[which(!is.na(stringr::str_match(data$id, "#1-(.*)")[,2])),]

data$content <- gsub("[^[:alnum:][:blank:]_]", "", data$content)


library(data.table)
data <- data.table(data)
tok_data <- data[, strsplit(x = content, split = ' ', perl = T), by = c('id','date')]
names(tok_data) <- c('id','date','token')


vocab <- unique(tok_data$token)

spec_char <- vocab[which(vocab%like%'_')]
spec_char_first <- spec_char[which(substring(spec_char,first = 1,last=1)=='_')]
spec_char_last_ind <- unlist(lapply(spec_char, function(x) substring(x,first = nchar(x),last=nchar(x))))
spec_char_last <- spec_char[which(spec_char_last_ind=='_')]

if(length(spec_char_first)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_first),]
}

if(length(spec_char_last)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_last),]
}

# remove empty rows 
empty_ind <- which(tok_data$token=='')
if(length(empty_ind)!=0){
  tok_data <- tok_data[-empty_ind,]
}

# remove NA-tokens
na_ind <- which(is.na(tok_data$token))
if(length(na_ind)!=0){
  tok_data <- tok_data[-na_ind,]
}


data <- tok_data[, text := paste0(token, collapse = ' '), by = c('id','date')]
data[, token := NULL]
data <- unique(data, by = 'id')


write.table(data,file = paste0("/home/r4/Documents/data_media_group_threat/bodytext_preproc5/DAGENS NYHETER_part1.txt"),sep="\t",row.names=FALSE,col.names = FALSE)


#---------------------------
# SVENSKA DAGBLADER
#---------------------------

# EDITORIALS
library('dplyr')
data_path <- '/data/data_media_group_threat/bodytext_preproc4/SVENSKA DAGBLADET.txt'

message('Read in data')
# all labled files
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

data <- data[which(!is.na(stringr::str_match(data$id, "#1-(.*)")[,2])),]

data$content <- gsub("[^[:alnum:][:blank:]_]", "", data$content)


library(data.table)
data <- data.table(data)
data <- unique(data)
tok_data <- data[, strsplit(x = content, split = ' ', perl = T), by = c('id','date')]
names(tok_data) <- c('id','date','token')


vocab <- unique(tok_data$token)

spec_char <- vocab[which(vocab%like%'_')]
spec_char_first <- spec_char[which(substring(spec_char,first = 1,last=1)=='_')]
spec_char_last_ind <- unlist(lapply(spec_char, function(x) substring(x,first = nchar(x),last=nchar(x))))
spec_char_last <- spec_char[which(spec_char_last_ind=='_')]


if(length(spec_char_first)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_first),]
}

if(length(spec_char_last)!=0){
  tok_data <- tok_data[-which(tok_data$token%in%spec_char_last),]
}

# remove empty rows 
empty_ind <- which(tok_data$token=='')
if(length(empty_ind)!=0){
  tok_data <- tok_data[-empty_ind,]
}

# remove NA-tokens
na_ind <- which(is.na(tok_data$token))
if(length(na_ind)!=0){
  tok_data <- tok_data[-na_ind,]
}
data <- tok_data[, text := paste0(token, collapse = ' '), by = c('id','date')]
data[, token := NULL]
data <- unique(data, by = 'id')


write.table(data,file = paste0("/home/r4/Documents/data_media_group_threat/bodytext_preproc5/SVENSKA DAGBLADET_part1.txt"),sep="\t",row.names=FALSE,col.names = FALSE)

