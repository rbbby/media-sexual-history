days <- function(year = 1945,
                 paper = 'AFTONBLADET') {
  
  if(year==2019){
    days <- seq(as.Date(paste0(as.character(year), '-01-01')),
                as.Date(paste0(as.character(year), '-05-31')),
                by = '+1 day')
  } else {
    days <- seq(as.Date(paste0(as.character(year), '-01-01')),
                as.Date(paste0(as.character(year), '-12-31')),
                by = '+1 day')
  }
  
  df <- data.frame(paper = rep(paper, length(days)),
                   dates = days)
  
  return(df)
}


afb_dates <- lapply(c(1945:2019), function(y) days(year = y, paper = 'AFTONBLADET'))
afb_dates <- data.table::rbindlist(afb_dates)

write.csv(afb_dates, 
          file = '/home/r4/Documents/full_data_20191118/AFTONBLADET/dates_afb.csv', 
          row.names = F)

dn_dates <- lapply(c(1945:2019), function(y) days(year = y, paper = 'DAGENS NYHETER'))
dn_dates <- data.table::rbindlist(dn_dates)

write.csv(dn_dates, 
          file = '/home/r4/Documents/full_data_20191118/DAGENS NYHETER/dates_dn.csv', 
          row.names = F)

exp_dates <- lapply(c(1945:2019), function(y) days(year = y, paper = 'EXPRESSEN'))
exp_dates <- data.table::rbindlist(exp_dates)

write.csv(exp_dates, 
          file = '/home/r4/Documents/full_data_20191118/EXPRESSEN/dates_exp.csv', 
          row.names = F)


svd_dates <- lapply(c(1945:2019), function(y) days(year = y, paper = 'SVENSKA DAGBLADET'))
svd_dates <- data.table::rbindlist(svd_dates)

write.csv(svd_dates, 
          file = '/home/r4/Documents/full_data_20191118/SVENSKA DAGBLADET/dates_svd.csv', 
          row.names = F)


dates <- dplyr::bind_rows(afb_dates, dn_dates,exp_dates, svd_dates)

write.csv(dates, 
          file = '/home/r4/Documents/full_data_20191118/dates_all.csv', 
          row.names = F)




