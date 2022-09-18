package com.will.scala

object TypeParam{
    def main(args: Array[String]): Unit = {
        val q = new Queue(List(1, 2), List(3))
        println(q.head)
        println(q.tail)
        println(q.tail)
        println(q.tail)
        println(q.tail)
        println(q.tail)
        println(q.enqueue(4))
        println(q.head)
        println(q.tail)
        println(q.enqueue(5))
        println(q.head)

        println(q.enqueue(6))
    }

    class Queue[T](
                      private val leading: List[T],
                      private val trailing: List[T]
                  ) {
        private def mirror =
            if (leading.isEmpty){
                new Queue(trailing.reverse, Nil)
            }else{
                this
            }
        def head = mirror.leading.head
        def tail = {
            val q = mirror
            new Queue(q.leading.tail, q.trailing)
        }
        def enqueue(x: T) = new Queue(leading, x::trailing)

        override def toString: String = {
            var leadingStr=""
            var trailingStr=""
            if(!leading.isEmpty){
                leadingStr = leading.toString()
            }
            if(!trailing.isEmpty){
                trailingStr = trailing.toString()
            }
            "leading:" + leadingStr + "\t" + "tail:" + trailingStr
        }
    }
}
