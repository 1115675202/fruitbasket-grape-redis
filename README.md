Jedis Pipeline 工具
===
## 简介
* 项目基于 Spring Redis Pipeline 进行了进一步封装，简洁易用灵活
## 环境及依赖
Jdk 1.8
## 运行步骤

## 目录结构描述
``` lua
cn.fruitbasket.grape.jedis.pipeline
├── AbstractJedisOps.java            操作抽象
├── JedisPipeline.java               经封装后给外部调用的管道对象
├── HashOps.java                     对应类型操作
├── ListOps.java                     对应类型操作
├── SetOps.java                      对应类型操作
├── ValueOps.java                    对应类型操作
└── ZSetOps.java                     对应类型操作
```
