library(data.table)
library(dplyr)
library(randomForest)
library(parallel)

####################
# FUNCTIONS
####################



# FIX NAMES/DASHES

read_in_token_error_files <- function(token_errors_folder = token_errors_folder){
  checkmate::assert_directory_exists(token_errors_folder)
  
  files <- dir(token_errors_folder, full.names = TRUE)
  
  raw <- list()
  for(i in seq_along(files)){
    raw[[i]] <- dplyr::as_tibble(read.csv(files[i], stringsAsFactors = FALSE, encoding = 'UTF-8'))
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
  tkns <- nrow(txt)
  message(tkns, " tokens in data.")
  
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

bodytext_replace_token_errors <- function(corpus, token_errors_folder, rare_word_limit, stop_list){
  #checkmate::assert_class(corpus, "tbl_df")
  checkmate::assert_directory_exists(token_errors_folder)
  
  errored_tokens <- read_in_token_error_files(token_errors_folder = token_errors_folder)
  checkmate::assert(!any(duplicated(errored_tokens$wrong_token)))
  
  # Tokenize corpus
  tok_content <- suppressMessages(tokenize_bodytext(corpus, rare_word_limit = rare_word_limit, stop_list = stop_list))
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
  
  corpus$content <- corpus$content_new
  corpus$content_new <- NULL
  
  # Trim 
  corpus$content <- stringr::str_trim(corpus$content)
  
  return(corpus)
}


remove_stopwords <- function(corpus, 
                             token_errors_folder = '/home/r4/Documents/git/kbdata/sources',
                             rare_word_limit = 0,
                             stop_list = stopwords_se){
  checkmate::assert_character(corpus$content)
  
  message('Removing stopwords and rare words')
  corpus <- bodytext_replace_token_errors(corpus, token_errors_folder = token_errors_folder, rare_word_limit = rare_word_limit,stop_list = stop_list)
  
  # Final trimming of space
  corpus$content <- stringr::str_replace_all(corpus$content, "[:space:]+", " ")
  corpus$content <- stringr::str_trim(corpus$content)
  
  return(corpus)
}



# MAIN FUNCTION
final_preproc_file <- function(base_path,
                               newspaper,
                               save_path,
                               rare_words_limits = 10,
                               stop_list = NULL) {
  
  # READ DATA
  message('Read data')
  # all labled files
  files <- list.files(paste0(base_path, newspaper, '/'))
  #  files <- files[i:j]
  
  # Read data 
  full_files <- paste0(base_path, newspaper,'/',files)
  
  cores <- 15
  gc()
  cl <- makeCluster(cores)
  system.time(dts <- parLapply(cl = cl,
                               X = full_files,
                               fun = function(x) try(readRDS(x))))
  stopCluster(cl)
  
  dts <- dts[sapply(dts,is.data.frame)]
  dts <- data.table::rbindlist(dts, fill = TRUE)
  
  # REMOVE STOPWORDS AND RARE WORDS
  if(!is.na(stop_list)){
    stopwords_se <- readLines(stop_list,
                              encoding = 'UTF-8')
  } else {
    stopwords_se <- NULL
  }
  
  tokenerrorfolder <- '/home/r4/Documents/git/kbdata/sources/errors'
  cores <- 14
  gc()
  
  dts <- remove_stopwords(corpus = dts, 
                           token_errors_folder = tokenerrorfolder,
                           rare_word_limit = 10,
                           stop_list = stopwords_se)
  
  
  
  
  ## SAVE data

  saveRDS(dts, file = paste0(save_path,newspaper,'/',newspaper,'.rds'))
  gc()
  
  
  
}




################
#  CLEAN DATA
###############

# AFTONBLADET
paper <- 'AFTONBLADET'

final_preproc_file(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/',
                   newspaper = paper,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                   rare_words_limits = 10,
                   stop_list = '/home/r4/Documents/git/kbdata/sources/se.txt')



# SVENSKA DAGBLADET
paper <- 'SVENSKA DAGBLADET'

final_preproc_file(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/',
                   newspaper = paper,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                   rare_words_limits = 10,
                   stop_list = '/home/r4/Documents/git/kbdata/sources/se.txt')


# EXPRESSEN
paper <- 'EXPRESSEN'

final_preproc_file(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/',
                   newspaper = paper,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                   rare_words_limits = 10,
                   stop_list = '/home/r4/Documents/git/kbdata/sources/se.txt')



# DAGENS NYHETER
paper <- 'DAGENS NYHETER'

final_preproc_file(base_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc1/',
                   newspaper = paper,
                   save_path = '/home/r4/Documents/data_media_group_threat/bodytext_preproc2/',
                   rare_words_limits = 10,
                   stop_list = '/home/r4/Documents/git/kbdata/sources/se.txt')







