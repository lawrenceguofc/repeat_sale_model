######## This is the driver file
########
########
library(readxl)
library(data.table)
options(warn=-1)
options(verbose=FALSE)
map <- read_excel('/Users/lguo/Documents/Listing Coverage/ListingStudy.Oct.2015.xls',sheet=1,skip=2)
map <- map[,c('CBSA Code','CBSA Title','County/County Equivalent','FIPS','Metropolitan/Micropolitan Statistical Area')]
setnames(map,old=c('County/County Equivalent','Metropolitan/Micropolitan Statistical Area'),new=c('county','msa'))
setnames(map,old=colnames(map),new=tolower(gsub(' ','_',colnames(map))))
map <- map[!is.na(map$fips),]
# change FIP code for Miami Date County
map[map$county=='Miami-Dade County','fips'] = '12025'

setwd('/Users/lguo/Documents/rpi/')
source('repeat_sale_pairing_function_cbsa.R') 
source('pair_function.R')
source('calculate_index.R')

# try for each cbsa code
all_cbsa <- unique(map[map$msa=='Metropolitan Statistical Area',]$cbsa_code)
all_fip <- unique(map$fips)

# pull 

rpi_msa_ls <- lapply(all_cbsa[1:2],function(cbsa_code){
  print (cbsa_code)
  try(repeat_sale_pair(cbsa_code=cbsa_code,month = 'Apr 2016'),silent=FALSE)
  })
names(rpi_msa_ls) <- all_cbsa
# function to convert a ts object to data.frame object
ts_to_df <- function(i,list){
 cbsa_code = names(list[i])
 ts = list[[i]]
  if(class(ts)=='ts'){
    month = as.character(as.yearmon(seq(tsp(ts)[1],tsp(ts)[2],by = 1/12)))
    df = data.frame(cbsa_code = cbsa_code,month = month,rpi = as.numeric(ts),stringsAsFactors=FALSE)
    df
  }
}

rpi_msa_ls2 <- lapply(1:length(rpi_msa_ls),function(i) ts_to_df(i,rpi_msa_ls))
rpi_msa_dt <- Reduce(function(x,y) rbindlist(list(x,y),fill=TRUE),rpi_msa_ls2)
rpi_msa_dt <- dcast(rpi_msa_dt,month~cbsa_code,value.var='rpi')
rpi_msa_dt <- rpi_msa_dt[with(rpi_msa_dt,order(as.yearmon(month,format='%b %Y'),decreasing=FALSE)),]

# merge map
cbsa <- map[map$msa=='Metropolitan Statistical Area',c('cbsa_title','cbsa_code')]
cbsa <- cbsa[!duplicated(cbsa),]
exclude_msa <- c('Amarillo, TX','Athens-Clarke County, GA','East Stroudsburg, PA',
                                    'Fort Collins, CO','Killeen-Temple, TX','Kingsport-Bristol-Bristol, TN-VA','Lawton, OK',
                                    'Lebanon, PA','Longview, TX','Lynchburg, VA','Monroe, MI','Montgomery, AL','Rockford, IL',
                                    'Scranton--Wilkes-Barre--Hazleton, PA','Springfield, MA','St. George, UT')
rpi_msa_dt <- merge(rpi_msa_dt,data.frame(cbsa),by='cbsa_code')
rpi_msa_dt <- rpi_msa_dt[!rpi_msa_dt$cbsa_title %in% exclude_msa, ]

saveRDS(rpi_msa_dt,'market_level_index.RDS')
saveRDS(rpi_cnty_dt,'county_level_index.RDS')

write.csv(rpi_msa_dt,'/Users/lguo/Desktop/rpi_msa_level.csv')

# 
all_fip <- map[map$cbsa_code %in% unique(rpi_msa_dt$cbsa_code),]$fips

rpi_cnty_ls <- lapply(all_fip,function(fip){
  print (fip)
  try(repeat_sale_pair(fip=fip),silent=FALSE)
})

names(rpi_cnty_ls) <- all_fip
rpi_cnty_ls2 <- lapply(1:length(rpi_cnty_ls),function(i) ts_to_df(i,rpi_cnty_ls))
rpi_cnty_dt <- Reduce(function(x,y) rbindlist(list(x,y),fill=TRUE),rpi_cnty_ls2)
colnames(rpi_cnty_dt)[1] <- 'fip'
rpi_cnty_dt <- merge(data.frame(rpi_cnty_dt),data.frame(map),by.x='fip',by.y='fips')

                      

