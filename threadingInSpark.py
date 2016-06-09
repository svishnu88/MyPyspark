import _thread

dflist=[]

def f(x):
    dflist.append(sqlContext.read.format("com.databricks.spark.csv").load(x))

l = ["/Users/vishnu/tmp/order_1.csv","/Users/vishnu/tmp/order_1.csv","/Users/vishnu/tmp/order_1.csv"]

for i in range(len(l))
    _thread.start_new_thread( f, (l[i],))

finalDf = sqlContext.createDataFrame(sc.emptyRDD(), dflist[0].schema)

for df in dflist:
    finalDf = finalDf.unionAll(df)
