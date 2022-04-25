library(data.table)
library(dplyr)
library(randomForest)
library(parallel)

####################
# FUNCTIONS
####################

# DATA CLEANING FUNCS

bodytext_clean <- function(corpus){
  checkmate::assert_character(corpus$content)
 
  corpus$content <- bodytext_clean_formating(corpus$content)
  corpus$content <- bodytext_clean_correct_errors(corpus$content)
  corpus$content <- bodytext_handle_abbreviations(corpus$content)

  corpus$content <- tolower(corpus$content)
  corpus$content <- bodytext_clean_symbols(corpus$content)
  corpus$content <- bodytext_clean_punctuation(corpus$content)
  corpus$content <- bodytext_clean_handle_digits(corpus$content)
  
  
  # Final trimming of space
  corpus$content <- stringr::str_replace_all(corpus$content, "[:space:]+", " ")
  corpus$content <- stringr::str_trim(corpus$content)
  
  return(corpus)
}

bodytext_clean_formating <- function(bodytext){
  checkmate::assert_character(bodytext)

  # Remove common "layout-characters"
  bodytext <- stringr::str_replace_all(bodytext, "■", " ")
  bodytext <- stringr::str_replace_all(bodytext, "\\*", " ")  
  bodytext <- stringr::str_replace_all(bodytext, "¬", "")  
  bodytext <- stringr::str_replace_all(bodytext, "»", "") 
  bodytext <- stringr::str_replace_all(bodytext, "\\^", "")
  bodytext <- stringr::str_replace_all(bodytext, "•", "")
  bodytext <- stringr::str_replace_all(bodytext, "~", "")
  bodytext <- stringr::str_replace_all(bodytext, "►", "")
  bodytext <- stringr::str_replace_all(bodytext, "«", "") 
  bodytext <- stringr::str_replace_all(bodytext, "£", "") 
  bodytext <- stringr::str_replace_all(bodytext, "®", "") 
  bodytext <- stringr::str_replace_all(bodytext, "#", "") 
  bodytext <- stringr::str_replace_all(bodytext, "¥", "") 
  bodytext <- stringr::str_replace_all(bodytext, "|", "") 
  bodytext <- stringr::str_replace_all(bodytext, "§", "") 
  bodytext <- stringr::str_replace_all(bodytext, "©", "") 
  
  # Remove em signs
  bodytext <- stringr::str_replace_all(bodytext, "<[//]?em>", " ")

 
  return(bodytext)
}

bodytext_clean_correct_errors <- function(bodytext){
  checkmate::assert_character(bodytext)
  # Small corrections in text
  bodytext <- stringr::str_replace_all(bodytext,
                                            "\\(TT ", 
                                            "\\(TT\\)")
  
  bodytext <- stringr::str_replace_all(bodytext,
                                       "c :a", 
                                       "c:a")
  
  bodytext <- stringr::str_replace_all(bodytext,
                                       " a,tt ", 
                                       " att ")

  return(bodytext)
}

bodytext_clean_handle_digits <- function(bodytext){
  
  # Handle digits
  bodytext <- stringr::str_replace_all(bodytext, "(1)[:digit:]{3}|(20)[:digit:]{2}", "YYYY")
  bodytext <- stringr::str_replace_all(bodytext, "(1)[:digit:]{3}|(19)[:digit:]{2}", "YYYY")
  bodytext <- stringr::str_replace_all(bodytext, "[:digit:][:punct:][:digit:]", "N_N")
  bodytext <- stringr::str_replace_all(bodytext, "[:digit:]", "N")
  bodytext <- stringr::str_replace_all(bodytext, "½", " N_N ")
  bodytext <- stringr::str_replace_all(bodytext, "¼", " N_N ")
  return(bodytext)
}

bodytext_clean_symbols <- function(bodytext){
  
  # Write out symbols
  bodytext <- stringr::str_replace_all(bodytext, "\\%", " procent ")
  bodytext <- stringr::str_replace_all(bodytext, "\\+", " plus ")
  bodytext <- stringr::str_replace_all(bodytext, "\\=", " lika_med ")
  bodytext <- stringr::str_replace_all(bodytext, "\\±", " plus_minus ")
  
  ## Remove special symbols/citations
  bodytext <- stringr::str_replace_all(bodytext, "\\`|\\´|\\¹", " ")
  
  # Remove apostophes
  bodytext <- stringr::str_replace_all(bodytext, "à|á", "a")
  bodytext <- stringr::str_replace_all(bodytext, "é|è", "e")
  bodytext <- stringr::str_replace_all(bodytext, "ó|ò", "o")
  
  
  bodytext
}

bodytext_clean_punctuation <- function(bodytext){
  checkmate::assert_character(bodytext)
  
  # Handle :
  bodytext <- stringr::str_replace_all(bodytext, " s:t ", " st ") # Special
  bodytext <- stringr::str_replace_all(bodytext, " c:a ", " ca ") # Special
  remove <- "s|n|are|en|et|erna|ar|ars|arna|andet|ande|at|ad|ade|er|ares|ns|ares|na|arnas|arnas|or|ans|as|a|orna|an|aren|ens|aren|erier|eriet|ets|e|t"
  bodytext <- stringr::str_replace_all(bodytext, paste0(":(",remove, ")( |$)"), "")
  
  # Handle weird OCR-stuff
  bodytext <- stringr::str_replace_all(bodytext, "&gt;", " ")
  odytext <- stringr::str_replace_all(bodytext, "&lt;", " ")
  
  
  # Handle . in web adresses
  bodytext <- stringr::str_replace_all(bodytext, "([:alpha:]+)(\\.)(com|dk|nu|se|org|net|fi|no)" , "\\1_\\3")
  
  # Clean up dashes (don't remove them)
  bodytext <- stringr::str_replace_all(bodytext, "‒|–|—|―|-|-", "-")
  bodytext <- stringr::str_replace_all(bodytext, "-+", "-")
  
  # Clean up single underscores (OBS! underscores are used for abbreviations)
  bodytext <- stringr::str_replace_all(bodytext, " _ ", " ")
  
  # Handle other punctuations
  # Don't use [:punct:] since that also removes underscore
  bodytext <- stringr::str_replace_all(bodytext, "[?!&,;:'\")(/@]", " ")
  bodytext <- stringr::str_replace_all(bodytext, "·", " ")
  bodytext <- stringr::str_replace_all(bodytext, "\\.", " ")
  bodytext <- stringr::str_replace_all(bodytext, "\\*", " ")
  bodytext <- stringr::str_replace_all(bodytext, "\\]", " ")
  bodytext <- stringr::str_replace_all(bodytext, "\\[", " ")
  bodytext <- stringr::str_replace_all(bodytext, "\\\\", " ")
  bodytext <- stringr::str_replace_all(bodytext, "\\°", " ")
  
  
  bodytext
}

bodytext_handle_abbreviations <- function(bodytext){
  checkmate::assert_character(bodytext)
  ## Handle punctuations
  
  # Handle common abbrv (with .) by tokenizing those
  bodytext <- stringr::str_replace_all(bodytext, " t\\.ex", " t_ex")
  bodytext <- stringr::str_replace_all(bodytext, " t ex ", " t_ex ")
  bodytext <- stringr::str_replace_all(bodytext, " bl\\.a", " bl_a")
  bodytext <- stringr::str_replace_all(bodytext, " bl a ", " bl_a ")
  bodytext <- stringr::str_replace_all(bodytext, " m\\.m", " m_m")
  bodytext <- stringr::str_replace_all(bodytext, " s\\.k", " s_k")   
  bodytext <- stringr::str_replace_all(bodytext, " s k ", " s_k ")  
  bodytext <- stringr::str_replace_all(bodytext, " t\\.o\\.m", " t_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " t\\.om", " t_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " t o m ", " t_o_m ")
  bodytext <- stringr::str_replace_all(bodytext, " fr\\.o\\.m", " fr_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " fr\\.om", " fr_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " fr o m ", " fr_o_m ")
  bodytext <- stringr::str_replace_all(bodytext, " m\\.fl", " m_fl")
  bodytext <- stringr::str_replace_all(bodytext, " f\\.d", " f_d")
  bodytext <- stringr::str_replace_all(bodytext, " o\\.d", " o_d")
  bodytext <- stringr::str_replace_all(bodytext, " e\\.d", " e_d")
  bodytext <- stringr::str_replace_all(bodytext, " kl\\.", " kl ")  
  bodytext <- stringr::str_replace_all(bodytext, " t\\.v", " t_v ")
  bodytext <- stringr::str_replace_all(bodytext, " t\\.h", " t_h ")
  bodytext <- stringr::str_replace_all(bodytext, " m m ", " m_m ")  
  bodytext <- stringr::str_replace_all(bodytext, " t v ", " t_v ") 
  bodytext <- stringr::str_replace_all(bodytext, " t h ", " t_h ")
  bodytext <- stringr::str_replace_all(bodytext, " o\\.s\\.v"," o_s_v")
  bodytext <- stringr::str_replace_all(bodytext, " o s v "," o_s_v ")
  bodytext <- stringr::str_replace_all(bodytext, " civ\\.ing"," civ_ing")
  bodytext <- stringr::str_replace_all(bodytext, " d\\.v\\.s"," d_v_s")
  bodytext <- stringr::str_replace_all(bodytext, " d v s "," d_v_s")
  bodytext <- stringr::str_replace_all(bodytext, " f\\.d"," f_d")
  bodytext <- stringr::str_replace_all(bodytext, " i\\.o\\.m"," i_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " i o m "," i_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " o\\.dyl"," o_dyl")
  bodytext <- stringr::str_replace_all(bodytext, " p\\.g\\.a"," p_g_a")
  bodytext <- stringr::str_replace_all(bodytext, " p g a "," p_g_a ")
  bodytext <- stringr::str_replace_all(bodytext, " ö\\.h\\.t "," ö_h_t ")
  bodytext <- stringr::str_replace_all(bodytext, " ö h t "," ö_h_t ")
  
  bodytext <- stringr::str_replace_all(bodytext, " T\\.ex", " T_ex")
  bodytext <- stringr::str_replace_all(bodytext, " T ex ", " T_ex") 
  bodytext <- stringr::str_replace_all(bodytext, " Bl\\.a", " Bl_a")
  bodytext <- stringr::str_replace_all(bodytext, " Bl a ", " Bl_a ")
  bodytext <- stringr::str_replace_all(bodytext, " M\\.m", " M_m")
  bodytext <- stringr::str_replace_all(bodytext, " S\\.k", " S_k")    
  bodytext <- stringr::str_replace_all(bodytext, " T\\.o\\.m", " T_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " T\\.om", " T_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " T o m ", " T_o_m ")
  bodytext <- stringr::str_replace_all(bodytext, " Fr\\.o\\.m", " Fr_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " Fr\\.om", " Fr_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " Fr o m ", " Fr_o_m ")
  bodytext <- stringr::str_replace_all(bodytext, " M\\.fl", " M_fl")
  bodytext <- stringr::str_replace_all(bodytext, " F\\.d", " F_d")
  bodytext <- stringr::str_replace_all(bodytext, " O\\.d", " O_d")
  bodytext <- stringr::str_replace_all(bodytext, " E\\.d", " E_d")
  bodytext <- stringr::str_replace_all(bodytext, " Kl\\.", " Kl ")    
  bodytext <- stringr::str_replace_all(bodytext, " T\\.v", " T_v ")
  bodytext <- stringr::str_replace_all(bodytext, " T\\.h", " T_h ")
  bodytext <- stringr::str_replace_all(bodytext, " T v ", " T_v ") 
  bodytext <- stringr::str_replace_all(bodytext, " T h ", " T_h ")
  bodytext <- stringr::str_replace_all(bodytext, " O\\.s\\.v"," O_s_v")
  bodytext <- stringr::str_replace_all(bodytext, " O s v "," O_s_v ")
  bodytext <- stringr::str_replace_all(bodytext, " D\\.v\\.s"," D_v_s")
  bodytext <- stringr::str_replace_all(bodytext, " F\\.d"," F_d")
  bodytext <- stringr::str_replace_all(bodytext, " I\\.o\\.m"," I_o_m")
  bodytext <- stringr::str_replace_all(bodytext, " O\\.dyl"," O_dyl")
  bodytext <- stringr::str_replace_all(bodytext, " P\\.g\\.a"," P_g_a")
  bodytext <- stringr::str_replace_all(bodytext, " P g a "," P_g_a ")
  bodytext <- stringr::str_replace_all(bodytext, " Ö\\.h\\.t "," Ö_h_t ")
  bodytext <- stringr::str_replace_all(bodytext, " Ö h t "," Ö_h_t ")
  
  return(bodytext)
}


# REMOVE EMPTY TEXT BLOCKS
bodytext_remove_observations <- function(corpus){
  # Remove empty textblocks
  to_remove <- corpus$content == "null" | stringr::str_count(corpus$content, " ") == 0 | nchar(corpus$content) == 0
  corpus <- corpus[!to_remove, ]
  return(corpus)
}


# FIX NAMES/DASHES

read_in_token_error_files <- function(token_errors_folder){
  checkmate::assert_directory_exists(token_errors_folder)
  
  files <- dir(token_errors_folder, full.names = TRUE)
  
  raw <- list()
  for(i in seq_along(files)){
    raw[[i]] <- dplyr::as_tibble(read.csv(files[i], stringsAsFactors = FALSE))
    checkmate::assert_class(raw[[i]], "tbl_df")
    checkmate::assert_names(names(raw[[i]]), identical.to =  c("wrong_token", "true_token"))
    checkmate::assert_character(raw[[i]]$wrong_token)
    checkmate::assert_character(raw[[i]]$true_token)
  }
  results <- dplyr::bind_rows(raw)
  
  # Remove duplicates
  results <- results[!duplicated(results),]
  return(results)
}

tokenize_bodytext <- function(corpus, rare_word_limit = 10, stop_list = NULL){
  #checkmate::assert_class(corpus, "tbl_df")
  checkmate::assert_subset(c("id", "content"), names(corpus))
  checkmate::assert_integerish(rare_word_limit, lower = 0)
  checkmate::assert_character(stop_list, any.missing = FALSE, null.ok = TRUE)
  
  # Create one-token-per-row object
  txt <- dplyr::tibble(textblock_id = as.factor(corpus$id), txt = corpus$content)
  txt <- tidytext::unnest_tokens(txt, word, txt, token = stringr::str_split, pattern = " ", to_lower = FALSE)
  #tkns <- nrow(txt)
  #message(tkns, " tokens in data.")
  
  # Remove stop words
  if(!is.null(stop_list)){
    txt$row <- 1L:nrow(txt)
    stop_word <- dplyr::data_frame(word = stop_list)
    txt <- dplyr::anti_join(txt, stop_word, by = "word")
    txt <- dplyr::arrange(txt, row)
    txt$row <- NULL
    message(tkns - nrow(txt), " stop word tokens removed.")
    tkns <- nrow(txt)
  }
  
  # Calculate and remove rare words
  if(rare_word_limit > 0){
    rare_words <- dplyr::count(txt, word) 
    rare_words <- dplyr::filter(rare_words, n <= rare_word_limit)
    txt$row <- 1L:nrow(txt)
    txt <- dplyr::anti_join(txt, rare_words, by = "word")
    txt <- dplyr::arrange(txt, row)
    txt$row <- NULL
    message(tkns - nrow(txt), " rare word tokens removed.")
    tkns <- nrow(txt)
  }
  
  names(txt) <- c("id", "token")
  txt$token <- as.factor(txt$token)
  return(txt)
}

bodytext_replace_token_errors <- function(corpus, token_errors_folder){
  #checkmate::assert_class(corpus, "tbl_df")
  checkmate::assert_directory_exists(token_errors_folder)
  
  errored_tokens <- read_in_token_error_files(token_errors_folder)
  checkmate::assert(!any(duplicated(errored_tokens$wrong_token)))
  
  # Tokenize corpus
  tok_content <- suppressMessages(tokenize_bodytext(corpus, 0))
  tok_content <- dplyr::mutate(tok_content, token = as.character(token))
  
  # Match with left_join
  tok_content <- dplyr::left_join(tok_content, errored_tokens, by = c("token" = "wrong_token"))
  
  # Choose token using ifelse
  tok_content$token[!is.na(tok_content$true_token)] <- 
  tok_content$true_token[!is.na(tok_content$true_token)]
  
  # Drop columns
  tok_content$true_token <- NULL
  
  # Paste together to full text again
  tok_content <- dplyr::group_by(tok_content, id)
  res <- dplyr::summarise(tok_content, content_new = paste(token, collapse = " "))
  rm(tok_content)
  res$id <- as.character(res$id)
  
  # Merge to corpus and remove old variable
  corpus$no <- 1:nrow(corpus)
  corpus <- dplyr::left_join(corpus, res, by = "id")
  corpus <- dplyr::arrange(corpus, no)
  corpus$no <- NULL
  
  # Bugg check
  # txt1 <- stringr::str_replace_all(corpus$anforandetext, "-|_", " ")
  # txt2 <- stringr::str_replace_all(corpus$anforandetext_new, "-|_", " ")
  # idx <- which(txt1 != txt2)
  
  corpus$content <- corpus$content_new
  corpus$content_new <- NULL
  
  # Trim 
  corpus$content <- stringr::str_trim(corpus$content)
  #corpus$content <- stringr::str_replace_all(corpus$content, "( )+", " ")
  
  return(corpus)
}


bodytext_replace_dash_tokens <- function(corpus){
  #checkmate::assert_class(corpus, "tbl_df")
  
  # Replace known errors
  corpus <- bodytext_replace_token_errors(corpus, token_errors_folder = "/home/r4/Documents/git/kbdata/sources/dash_errors/")
  
  # Combine the rest of the dash separations
  corpus$content <- stringr::str_replace_all(corpus$content, "-", "")
#  corpus$content <- stringr::str_replace_all(corpus$content, "[:space:]+", " ")
  
  return(corpus)
}



bodytext_replace_collocation <- function(corpus, 
                                         collocation_folder = '/home/r4/Documents/git/kbdata/sources/collocations'){
  checkmate::assert_character(corpus$content)
  checkmate::assert_directory_exists(collocation_folder)
  
  raw <- read_in_collocation_files(collocation_folder)
  
  from <- paste0("(^| )", raw, "($| )")#[1:200]
  to <- paste0(" ", stringr::str_replace_all(raw, " ", "_"), " ")#[1:200]
  for(i in seq_along(to)){
    corpus$content <- stringr::str_replace_all(corpus$content, from[i], to[i])
  }
  
  # Trim 
  corpus$content <- stringr::str_trim(corpus$content)
  #corpus$content <- stringr::str_replace_all(corpus$content, "( )+", " ")
  
  return(corpus)
}



read_in_collocation_files <- function(collocation_folder){
  checkmate::assert_directory_exists(collocation_folder)
  
  collocation_files <- dir(collocation_folder, full.names = TRUE)
  
  raw <- list()
  for(i in seq_along(collocation_files)){
    raw[[i]] <- readLines(collocation_files[i])
    checkmate::assert_character(raw[[i]], min.chars = 3, pattern = " ")
  }
  raw <- do.call(c,raw)
  
  # Remove duplicates
  return(raw[!duplicated(raw)])
}


bodytext_dash_control <- function(corpus){
  checkmate::assert_character(corpus$content)
  
  corpus <- bodytext_replace_dash_tokens(corpus)
  corpus <- bodytext_replace_collocation(corpus)
  
  
  # Final trimming of space
  corpus$content <- stringr::str_replace_all(corpus$content, "[:space:]+", " ")
  corpus$content <- stringr::str_trim(corpus$content)
  
  return(corpus)
}


# MASTER FUNCITIONS

clean_bodytext <- function(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                           newspaper = 'AFTONBLADET',
                           i = 1,
                           j = 500) {
  
  
  # all labled files
  files <- list.files(paste0(base_path, newspaper, '/'))
  files <- files[i:j]
  
  # Read data 
  message('Reading data')
  full_files <- paste0(base_path, newspaper,'/',files)
  
  cores <- 15
  gc()
  cl <- makeCluster(cores)
  system.time(dts <- parLapply(cl = cl,
                               X = full_files,
                               fun = function(x) try(readRDS(x))))
  stopCluster(cl)
  
  
  # Remove empty data
  message('Remove empty data')
  
  # Remove potentially empty textblocks
  cores <- 15
  gc()
  cl <- makeCluster(cores)
  var_func_list <- c('bodytext_remove_observations') 
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(dts <- parLapply(cl = cl,
                               X = dts,
                               fun = function(x) try(bodytext_remove_observations(corpus = x))))
  stopCluster(cl)
  
  
  # Preprocessing - cleaning data
  message('Clean data')
  
  cores <- 15
  gc()
  cl <- makeCluster(cores)
  var_func_list <- c('bodytext_clean','bodytext_clean_formating','bodytext_clean_correct_errors','bodytext_handle_abbreviations',
                     'bodytext_clean_symbols','bodytext_clean_punctuation','bodytext_clean_handle_digits') 
  clusterEvalQ(cl, {library(checkmate);  library(stringr)})
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(dts <- parLapply(cl = cl,
                               X = dts,
                               fun = function(x) try(bodytext_clean(corpus = x))))
  stopCluster(cl)
  
  names(dts) <- gsub(pattern = '.rds', replacement = '', x = files)
  
  return(dts)
  
}


bodytext_dash_control <- function(corpus){
  checkmate::assert_character(corpus$content)
  
  message('Replace dash tokens')
  corpus <- bodytext_replace_dash_tokens(corpus)
  message('Replace collocation')
  corpus <- bodytext_replace_collocation(corpus)
  
  
  # Final trimming of space
  corpus$content <- stringr::str_replace_all(corpus$content, "[:space:]+", " ")
  corpus$content <- stringr::str_trim(corpus$content)
  
  return(corpus)
}



################
#  CLEAN DATA
###############

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
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))
    # INITIAL CLEAN
    
    dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                          newspaper = paper,
                          i = i,
                          j = j)  
    
    
    ## SAVE MIDDLESTEP - 0
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'

    
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    
    
    # FIX WITH DASHES
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                        'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
    clusterEvalQ(cl, {library(checkmate);  library(stringr)})
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(dts <- parLapply(cl = cl,
                                 X = dts,
                                 fun = function(x) try(bodytext_dash_control(corpus = x))))
    stopCluster(cl)
    
    
    
    ## SAVE MIDDLESTEP 1
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'

    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    

    
    break
    
  }
  
  # INITIAL CLEAN
  
  dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                        newspaper = 'AFTONBLADET',
                        i = i,
                        j = j)  
  
  
  ## SAVE MIDDLESTEP - 0
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'

  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  
  
  
  # FIX WITH DASHES
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                      'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
  clusterEvalQ(cl, {library(checkmate);  library(stringr)})
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(dts <- parLapply(cl = cl,
                               X = dts,
                               fun = function(x) try(bodytext_dash_control(corpus = x))))
  stopCluster(cl)
  
  
  
  ## SAVE MIDDLESTEP 1
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'

  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  

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
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))
    # INITIAL CLEAN
    
    dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                          newspaper = paper,
                          i = i,
                          j = j)  
    
    
    ## SAVE MIDDLESTEP - 0
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'
    
    
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    
    
    # FIX WITH DASHES
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                        'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
    clusterEvalQ(cl, {library(checkmate);  library(stringr)})
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(dts <- parLapply(cl = cl,
                                 X = dts,
                                 fun = function(x) try(bodytext_dash_control(corpus = x))))
    stopCluster(cl)
    
    
    
    ## SAVE MIDDLESTEP 1
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'
    
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    
    
    
    break
    
  }
  
  # INITIAL CLEAN
  
  dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                        newspaper = paper,
                        i = i,
                        j = j)  
  
  
  ## SAVE MIDDLESTEP - 0
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'
  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  
  
  
  # FIX WITH DASHES
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                      'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
  clusterEvalQ(cl, {library(checkmate);  library(stringr)})
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(dts <- parLapply(cl = cl,
                               X = dts,
                               fun = function(x) try(bodytext_dash_control(corpus = x))))
  stopCluster(cl)
  
  
  
  ## SAVE MIDDLESTEP 1
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'
  
  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  
  

  
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
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))
    # INITIAL CLEAN
    
    dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                          newspaper = paper,
                          i = i,
                          j = j)  
    
    
    ## SAVE MIDDLESTEP - 0
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'
    
    
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    
    
    # FIX WITH DASHES
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                        'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
    clusterEvalQ(cl, {library(checkmate);  library(stringr)})
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(dts <- parLapply(cl = cl,
                                 X = dts,
                                 fun = function(x) try(bodytext_dash_control(corpus = x))))
    stopCluster(cl)
    
    
    
    ## SAVE MIDDLESTEP 1
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'
    
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    
    
    
    break
    
  }
  
  # INITIAL CLEAN
  
  dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                        newspaper = paper,
                        i = i,
                        j = j)  
  
  
  ## SAVE MIDDLESTEP - 0
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'
  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  
  
  
  # FIX WITH DASHES
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                      'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
  clusterEvalQ(cl, {library(checkmate);  library(stringr)})
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(dts <- parLapply(cl = cl,
                               X = dts,
                               fun = function(x) try(bodytext_dash_control(corpus = x))))
  stopCluster(cl)
  
  
  
  ## SAVE MIDDLESTEP 1
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'
  
  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  
  

  
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
  
  if(j > length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))) {
    j <- length(list.files(paste0('/home/r4/Documents/full_data_20191118/',paper,'/classified0/')))
    # INITIAL CLEAN
    
    dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                          newspaper = paper,
                          i = i,
                          j = j)  
    
    
    ## SAVE MIDDLESTEP - 0
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'
    
    
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    
    
    # FIX WITH DASHES
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                        'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
    clusterEvalQ(cl, {library(checkmate);  library(stringr)})
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(dts <- parLapply(cl = cl,
                                 X = dts,
                                 fun = function(x) try(bodytext_dash_control(corpus = x))))
    stopCluster(cl)
    
    
    
    ## SAVE MIDDLESTEP 1
    save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'
    
    cores <- 14
    gc()
    cl <- makeCluster(cores)
    var_func_list <-  c('dts','save_path','paper')
    clusterExport(cl = cl, varlist = var_func_list, envir = environment())
    
    system.time(parLapply(cl = cl,
                          X = names(dts),
                          fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
    stopCluster(cl)
    gc()
    
    
    
    break
    
  }
  
  # INITIAL CLEAN
  
  dts <- clean_bodytext(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_raw0/',
                        newspaper = paper,
                        i = i,
                        j = j)  
  
  
  ## SAVE MIDDLESTEP - 0
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc0/'
  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  
  
  
  # FIX WITH DASHES
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('read_in_token_error_files','tokenize_bodytext','bodytext_replace_token_errors','bodytext_replace_dash_tokens',
                      'bodytext_replace_collocation','read_in_collocation_files','bodytext_dash_control')
  clusterEvalQ(cl, {library(checkmate);  library(stringr)})
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(dts <- parLapply(cl = cl,
                               X = dts,
                               fun = function(x) try(bodytext_dash_control(corpus = x))))
  stopCluster(cl)
  
  
  
  ## SAVE MIDDLESTEP 1
  save_path <- '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/'
  
  
  cores <- 14
  gc()
  cl <- makeCluster(cores)
  var_func_list <-  c('dts','save_path','paper')
  clusterExport(cl = cl, varlist = var_func_list, envir = environment())
  
  system.time(parLapply(cl = cl,
                        X = names(dts),
                        fun = function(x) try(saveRDS(dts[[x]], file = paste0(save_path,paper,'/',x,'.rds')))))
  stopCluster(cl)
  gc()
  


  
}











