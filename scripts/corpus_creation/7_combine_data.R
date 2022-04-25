#---------------
# PART 1
#---------------

library(data.table)
library(dplyr)
# AFTONBLADET
path <- '/media/r3/Flyttflytt/data_media_group_threat/bodytext_preproc5/bodytext_preproc5/'
afb <- fread(paste0(path,'/AFTONBLADET_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
afb %>% mutate(id = paste0('AFTONBLADET_',id)) -> afb

afb <- data.table(afb)
afb <- unique(afb)
system.time(tok_afb <-  afb[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_afb) <- c('id','date','token')
system.time(n_token <- tok_afb[,.N, by = 'token'])
system.time(remove_token <- n_token$token[which(n_token$N<25)])
if(length(remove_token)>0){
  system.time(tok_afb <- tok_afb[-which(tok_afb$token%in%remove_token),])
}
system.time(afb <- tok_afb[,  paste0(token, collapse = ' '), by = c('id','date')])
#rm(tok_afb)



# EXPRESSEN
exp <- fread(paste0(path,'EXPRESSEN_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
exp %>% mutate(id = paste0('EXPRESSEN_',id)) -> exp


exp <- data.table(exp)
exp <- unique(exp)
system.time(tok_exp <-  exp[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_exp) <- c('id','date','token')
system.time(n_token <- tok_exp[,.N, by = 'token'])
system.time(remove_token <- n_token$token[which(n_token$N<25)])
if(length(remove_token)>0){
  system.time(tok_exp <- tok_exp[-which(tok_exp$token%in%remove_token),])
}
system.time(exp <- tok_exp[,  paste0(token, collapse = ' '), by = c('id','date')])
rm(tok_exp)

# SVENSKA DAGBLADET
svd <- fread(paste0(path,'SVENSKA DAGBLADET_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
svd %>% mutate(id = paste0('SVD_',id)) -> svd


svd <- data.table(svd)
svd <- unique(svd)
system.time(tok_svd <-  svd[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_svd) <- c('id','date','token')
system.time(n_token <- tok_svd[,.N, by = 'token'])
system.time(remove_token <- n_token$token[which(n_token$N<25)])
if(length(remove_token)>0){
  system.time(tok_svd <- tok_svd[-which(tok_svd$token%in%remove_token),])
}
system.time(svd <- tok_svd[,  paste0(token, collapse = ' '), by = c('id','date')])
rm(tok_svd)


# DAGENS NYHETER
dn <- fread(paste0(path,'DAGENS NYHETER_part1.txt'),
            header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
dn %>% mutate(id = paste0('DN_',id)) -> dn



dn <- data.table(dn)
dn <- unique(dn)
system.time(tok_dn <-  dn[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_dn) <- c('id','date','token')
system.time(n_token <- tok_dn[,.N, by = 'token'])
system.time(remove_token <- n_token$token[which(n_token$N<25)])
if(length(remove_token)>0){
  system.time(tok_dn <- tok_dn[-which(tok_dn$token%in%remove_token),])
}
system.time(dn <- tok_dn[,  paste0(token, collapse = ' '), by = c('id','date')])
rm(tok_dn)


# combine data

system.time(d <- dplyr::bind_rows(afb,exp,dn,svd))
write.table(d,file = '/home/r4/Documents/data_media_group_threat/bodytext_preproc5/part1.txt',sep="\t",row.names=FALSE,col.names = FALSE)

## test
d <- fread('/home/r4/Documents/data_media_group_threat/bodytext_preproc5/part1.txt')
names(d) <- c('id','date','content')


d <- d[,.N, by = 'date']
plot(x = as.Date(d$date), y = d$N)

##

path <- '/media/r3/Flyttflytt/data_media_group_threat/bodytext_preproc5/bodytext_preproc5/'

part1 <- fread(paste0(path, 'part1.txt'), header = T)
names(part1) <- c('id','date','content')

tokenize <- function(data){
  res <- data[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")]
  names(res) <- c('id','date','token')
  res <- res[,.N, 'token']
  return(res)
}

#test <- part1[1:10000,]
n <- 1000
nr <- nrow(part1)
system.time(part1 <- split(part1, rep(1:ceiling(nr/n), each=n, length.out=nr)))

library(parallel)
cores <- 15
gc()
cl <- makeCluster(cores)
var_func_list <- c('tokenize','data.table')
clusterExport(cl = cl, varlist = var_func_list, envir = environment())
system.time(part1 <- parLapply(cl = cl,
                                       X = part1,
                                       fun = function(x) tokenize(data = x)))

part1 <- rbindlist(part1, fill = TRUE)
n_token <- part1[, sum(N), by = 'token']
n_token %>% arrange(V1) -> n_token

n_token$token[which(n_token$V1<50 | n_token$V1>5000000)] -> n_token
data.table::fwrite(data.table(n_token), paste0(path,'remove_tokens.csv'))


system.time(tok <-  part1[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_dn) <- c('id','date','token')
system.time(n_token <- tok_dn[,.N, by = 'token'])



#--------------------------------------
# FIND VERY COMMON AND UNCOMMON WORDS
#       !!! RUN ONCE  !!!!
#-------------------------------

library(data.table)
library(dplyr)
# AFTONBLADET
path <- '/media/r3/Flyttflytt/data_media_group_threat/bodytext_preproc5/bodytext_preproc5/'
afb <- fread(paste0(path,'/AFTONBLADET_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
afb %>% mutate(id = paste0('AFTONBLADET_',id)) -> afb
afb <- data.table(afb)
afb <- unique(afb)

exp <- fread(paste0(path,'EXPRESSEN_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
exp %>% mutate(id = paste0('EXPRESSEN_',id)) -> exp
exp <- data.table(exp)
exp <- unique(exp)

svd <- fread(paste0(path,'SVENSKA DAGBLADET_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
svd %>% mutate(id = paste0('SVD_',id)) -> svd
svd <- data.table(svd)
svd <- unique(svd)


dn <- fread(paste0(path,'DAGENS NYHETER_part1.txt'),
            header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
dn %>% mutate(id = paste0('DN_',id)) -> dn
dn <- data.table(dn)
dn <- unique(dn)

# COMBINE
system.time(d <- dplyr::bind_rows(afb,exp,dn,svd))

rm(afb)
rm(exp)
rm(dn)
rm(svd)

gc()


# TOKENIZE
tokenize <- function(data){
  res <- data[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")]
  names(res) <- c('id','date','token')
  res <- res[,.N, by = 'token']
  
  gc()
  return(res)
  
}

#test <- part1[1:10000,]
n <- 1000
nr <- nrow(d)
system.time(d<- split(d, rep(1:ceiling(nr/n), each=n, length.out=nr)))

library(parallel)
cores <- 15
gc()
cl <- makeCluster(cores)
var_func_list <- c('tokenize','data.table')
clusterExport(cl = cl, varlist = var_func_list, envir = environment())
system.time(d <- parLapply(cl = cl,
                               X = d,
                               fun = function(x) tokenize(data = x)))

d <- rbindlist(d, fill = TRUE)



n_token <- d[,sum(N), by = 'token']
n_token %>% arrange(V1) -> n_token

n_token$token[which(n_token$V1<50 | n_token$V1>5000000)] -> n_token
write.table(data.table(n_token), paste0(path,'remove_tokens.csv'), fileEncoding = 'UTF-8', row.names = F, col.names = F)


#--------------------------------------
# REMOVE THE WORDS
#---------------------------------------'
library(data.table)
library(dplyr)
n_token <- fread(paste0(path,'remove_tokens.csv'), header = F)


# AFTONBLADET
path <- '/media/r3/Flyttflytt/data_media_group_threat/bodytext_preproc5/bodytext_preproc5/'
afb <- fread(paste0(path,'/AFTONBLADET_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
afb %>% mutate(id = paste0('AFTONBLADET_',id)) -> afb

afb <- data.table(afb)
afb <- unique(afb)
system.time(tok_afb <-  afb[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_afb) <- c('id','date','token')
system.time(remove_token <- which(tok_afb$token %in% n_token$V1))
if(length(remove_token)>0){
  system.time(tok_afb <- tok_afb[-remove_token,])
}
system.time(afb <- tok_afb[,  paste0(token, collapse = ' '), by = c('id','date')])
rm(tok_afb)



# EXPRESSEN
exp <- fread(paste0(path,'EXPRESSEN_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
exp %>% mutate(id = paste0('EXPRESSEN_',id)) -> exp


exp <- data.table(exp)
exp <- unique(exp)
system.time(tok_exp <-  exp[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_exp) <- c('id','date','token')
system.time(remove_token <- which(tok_exp$token %in% n_token$V1))
if(length(remove_token)>0){
  system.time(tok_exp <- tok_exp[-remove_token,])
}
system.time(exp <- tok_exp[,  paste0(token, collapse = ' '), by = c('id','date')])
rm(tok_exp)

# SVENSKA DAGBLADET
svd <- fread(paste0(path,'SVENSKA DAGBLADET_part1.txt'),
             header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
svd %>% mutate(id = paste0('SVD_',id)) -> svd


svd <- data.table(svd)
svd <- unique(svd)
system.time(tok_svd <-  svd[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_svd) <- c('id','date','token')
system.time(remove_token <- which(tok_svd$token %in% n_token$V1))
if(length(remove_token)>0){
  system.time(tok_svd <- tok_svd[-remove_token,])
}
system.time(svd <- tok_svd[,  paste0(token, collapse = ' '), by = c('id','date')])
rm(tok_svd)


# DAGENS NYHETER
dn <- fread(paste0(path,'DAGENS NYHETER_part1.txt'),
            header = FALSE) %>%
  rename(id = V1,
         date = V2,
         content = V3)
dn %>% mutate(id = paste0('DN_',id)) -> dn



dn <- data.table(dn)
dn <- unique(dn)
system.time(tok_dn <-  dn[, strsplit(x = content, split = ' ', perl = T), by=c("id","date")])
names(tok_dn) <- c('id','date','token')
system.time(remove_token <- which(tok_dn$token %in% n_token$V1))
if(length(remove_token)>0){
  system.time(tok_dn <- tok_dn[-remove_token,])
}
system.time(dn <- tok_dn[,  paste0(token, collapse = ' '), by = c('id','date')])
rm(tok_dn)


# combine data

system.time(d <- dplyr::bind_rows(afb,exp,dn,svd))
write.table(d,file = paste(path,'part1_new.txt',sep="\t",row.names=FALSE,col.names = FALSE)

