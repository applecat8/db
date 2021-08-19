# 使用c语言实现一个简单的数据库

[原教程](https://cstack.github.io/db_tutorial/)
以Sqlite为模型

## Part 1 - 介绍和设置REPL(交互命令解析器)

[原文part-1](https://cstack.github.io/db_tutorial/parts/part1.html)

## Pare 2 - 世界上最简单的 SQL 编译器和虚拟机

[原文part-2](https://cstack.github.io/db_tutorial/parts/part2.html)

![SQLite 架构](https://cstack.github.io/db_tutorial/assets/images/arch2.gif)

## Pare 3 - 一个内存中的、仅有应用的、单表的数据库

[原文part-3](https://cstack.github.io/db_tutorial/parts/part3.html)

实现了在内存的数据存储，以及列出数据

## Pare 4 - 测试程序和一些bug(边界条件)

[原文part-4](https://cstack.github.io/db_tutorial/parts/part4.html)

测试程序我使用的是js

## Pare 5 - 数据持久化

## Pare 6 - 添加一层抽象(游标)

[原文part-5](https://cstack.github.io/db_tutorial/parts/part5.html)
帮助我们更好的将数据结构重写为B-Tree

## Pare 7 B-Tree的介绍

* 为什么说树是数据库的一个好的数据结构？
  * 搜索一个特定的值是快速的（对数时间）。
  * 插入/删除一个你已经找到的值是快速的（重新平衡的时间是恒定的）。
  * 遍历一个值的范围是快速的（不像哈希图）。

与二叉树不同，B树中的每个节点可以有2个以上的孩子。每个节点最多可以有m个孩子，其中m被称为树的 "顺序"。为了保持树的基本平衡，我们还说节点必须至少有m/2个孩子（向上取整）

上面的图片是一个B树，SQLite用它来存储索引。为了存储表，SQLites使用了一种叫做B+树的变体。

|B-tree|B+tree|
-|-|-
Pronounced|"Bee Tree"|"Bee Plus Tree"
Used to store|Indexes|Tables
Internal nodes store keys|Yes|Yes
Internal nodes store values|Yes|No
Number of children per node|Less|More
Internal nodes vs. leaf nodes| Same structure|Different structure

B+树中内部节点和叶子节点的结构差异

For an order-m tree…  |  Internal Node |  Leaf Node
-|-|-
Stores | keys and pointers to children |  keys and values
Number of keys | up to m-1 |  as many as will fit
Number of pointers | number of keys + 1 | none
Number of values  |  none  |  number of keys
Key purpose  |   used for routing |   paired with value
Stores values? | No | Yes

* 例如:order为3的B+树的特点
  * 每个内部节点最多有3个孩子
  * 每个内部节点最多两个键
  * 每个内部节点至少有2个孩子
  * 每个内部节点至少有1个键
