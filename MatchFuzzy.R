library(reticulate)
library(fuzzywuzzyR)
library(gtools)


df1 = read.csv("df1.csv")
df2 = read.csv("df2.csv")

# uncomment these 2 lines for testing, select a small number of rows to reduce the running time
df1 = df1[1:8,]
df2 = df2[1:8,]

match <- function(df1, df2){
  df1$produs1 = as.character(df1$produs1)
  df2$produs2 = as.character(df2$produs2)
  
  lista = list()
  for (i in 1:nrow(df1)){
    lista[i] = ""
    for (j in 1:nrow(df2)){
      init = FuzzMatcher$new()
      #compute the mathcing score
      lista[[i]][j] = paste(init$Token_set_ratio(df1$produs1[i], df2$produs2[j]), df2$produs2[j]) 
    }
  }
  print(lista)
  lista = lapply(lista, gtools::mixedsort, decreasing = TRUE)
  #select first 3 records with the highest score 
  print(lista)
  for(i in 1:length(lista)){
    lista[[i]] = lista[[i]][1:3]
  }
  print(lista)
  return(lista)
}


#test the function, grab a coffe, takes a loooong time
list = match(df1, df2)


###### parallel version ########

library(parallel)
df1 <<- read.csv("df1.csv")
df2 <<- read.csv("df2.csv")

df1 <<- df1[1:8,]
df2 <<- df2[1:8,]


pmatch <- function(ichunks){
  df1$produs1 = as.character(df1$produs1)
  df2$produs2 = as.character(df2$produs2)
  
  lista = list()
  for (i in ichunks){
    lista[i] = ""
    for (j in 1:nrow(df2)){
      init = FuzzMatcher$new()
      #compute the mathcing score
      lista[[i]][j] = paste(init$Token_set_ratio(df1$produs1[i], df2$produs2[j]), df2$produs2[j]) 
    }
  }
  
  lista = lapply(lista, gtools::mixedsort, decreasing = TRUE)
  
  #select first 3 records with the highest score 
  #for(i in 1:length(lista)){
  #  lista[[i]] = lista[[i]][1:3]
  #}
  
  return(lista)
}


cls<-makeCluster(parallel::detectCores())
clusterEvalQ(cls, library(reticulate))
clusterEvalQ(cls, library(fuzzywuzzyR))
clusterEvalQ(cls, library(gtools))
clusterExport(cls, "df1")
clusterExport(cls, "df2")

ichunks<-clusterSplit(cls, 1:nrow(df1))
plista<-clusterApply(cls, ichunks, pmatch )

stopCluster(cls)
