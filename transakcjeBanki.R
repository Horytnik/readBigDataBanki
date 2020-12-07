install.packages("DBI")
install.packages("RSQLite")
install.packages("devtools")

library("RSQLite")
library("DBI")

dataSmall <- read.csv("transakcje.csv", nrows = 20)
View(dataSmall)

dataKonta <- read.csv("konta.csv")
View(dataKonta)

srednia <- function(filepath, columnname, size, sep=",")
{
  fileConnection <- file(description = filepath, open="r")
  suma <-0
  counter<-0

  data <- read.table(fileConnection, nrows = size, header = TRUE, fill = TRUE, sep = sep)
  
  columnNames <-names(data)
  repeat{
    
    if(nrow(data)==0)
    {
      close.connection(fileConnection)
      break
    }
    
    data<-na.omit(data)
    suma <- suma+sum(data[[columnname]])
    counter<- counter+nrow(data)
    print(counter)
    
    data <- read.table(fileConnection, nrows = size, col.names = columnNames, fill = TRUE, sep = sep)
  }
  return(suma/counter)
}

srednia2<- function(filepath,columnname,size,sep=","){
  fileConnection<-file(description = filepath,open="r")
  suma<-0
  counter<-0
  data<-read.table(fileConnection,nrows = size,header = TRUE,fill=TRUE,sep=sep )
  columnsNames<-names(data)
  repeat{
    if ( nrow(data)==0){
      close.connection(fileConnection)
      break
    }
    data<-na.omit(data)
    suma<- suma + sum(data[[columnname]])
    counter<- counter + nrow(data)
    print(counter)
    data<-read.table(fileConnection,nrows = size,col.names = columnsNames,fill=TRUE,sep=sep )
  }
  suma / counter
}


srednia(filepath= "transakcje.csv",columnname = 'amount',size= 100000)
srednia(filepath= "konta.csv",columnname = 'saldo',size= 1000)

readToBase<- function(filepath,dbpath,tableName,size,sep=","){
  fileConnection<-file(description  = filepath, open = "r")
  dbConn<-dbConnect(SQLite(),dbpath)
  data<-read.table(fileConnection, nrows =size, header = TRUE, fill = TRUE, sep=sep)
  columnsNames<-names(data)
  
  dbWriteTable(conn=dbConn, name = tableName,data,append=TRUE, overwrite = FALSE)
  
  repeat{
    
    if(nrow(data)==0){
      close(fileConnection)
      dbDisconnect(dbConn)
      break
    }
    data<-read.table(fileConnection, nrows =size, col.names = columnsNames, fill = TRUE, sep=sep)
    dbWriteTable(conn=dbConn, name = tableName,data,append=TRUE, overwrite = FALSE)
    
  }
}

readToBase("konta.csv","bazaKont.sqlite","konta",1000)
readToBase("transakcje.csv","bazaTransakcji.sqlite","transakcje",100000)


dbpath <-"bazaKont.sqlite"
dbconn <- dbConnect(SQLite(),dbpath)
dbListTables(dbconn)
salda <- dbGetQuery(conn = dbconn,"select saldo from konta")

saldaLimit <- dbGetQuery(conn = dbconn,"select * from konta limit 10")
dbDisconnect(dbconn)


