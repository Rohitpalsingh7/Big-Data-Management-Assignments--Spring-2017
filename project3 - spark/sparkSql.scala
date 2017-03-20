
case class Record(TransID: Int, CustID: Int, TransTotal: Float, TransNumItems:Int, TransDesc:String)

val data = sc.textFile("/home/mqp/Transactions").map(line => line.split(",")).map {
  case Array(r1,r2,r3,r4,r5) => Record(r1.toInt, r2.toInt, r3.toFloat, r4.toInt, r5)
}
val T0 = data.toDF()
val T1 = T0.where(T0("TransTotal") >= 250)
val T2 = T1.groupBy(T1("TransNumItems")).agg(sum("TransTotal"), avg("TransTotal"), max("TransTotal"), min("TransTotal"))
T2.show()

val T3 = T1.groupBy(T1("CustID")).count()
T3.show()
// to show all: T3.show(Int.MaxValue - 1)

val T4 = T0.where(T0("TransTotal") >= 600)
val T5 = T4.groupBy(T1("CustID")).count()
T5.show()
// to show all: T5.show(Int.MaxValue - 1)

val _T6 = T5.join(T3, "CustID").where(T5("count") * 3 < T3("count"))
_T6.show() 

val T6 = _T6.select("CustID")
T6.show()
// to show all: T6.show(Int.MaxValue - 1)   