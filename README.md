# apijson-influxdb  [![](https://jitpack.io/v/APIJSON/apijson-influxdb.svg)](https://jitpack.io/#APIJSON/apijson-influxdb)
腾讯 [APIJSON](https://github.com/Tencent/APIJSON) 6.1.0+ 的 InfluxDB 数据库插件，可通过 Maven, Gradle 等远程依赖。<br />
An InfluxDB plugin for Tencent [APIJSON](https://github.com/Tencent/APIJSON) 6.1.0+

![image](https://github.com/APIJSON/apijson-influxdb/assets/5738175/243d7a46-e035-4fe6-be63-51cb54d4a69d)
![image](https://github.com/APIJSON/apijson-influxdb/assets/5738175/3c2919b6-f90a-4a9e-9fb7-592eb4c1a6bb)


## 添加依赖
## Add Dependency

### Maven
#### 1. 在 pom.xml 中添加 JitPack 仓库
#### 1. Add the JitPack repository to pom.xml
```xml
	<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>
```

![image](https://user-images.githubusercontent.com/5738175/167261814-d75d8fff-0e64-4534-a840-60ef628a8873.png)

<br />

#### 2. 在 pom.xml 中添加 apijson-influxdb 依赖
#### 2. Add the apijson-influxdb dependency to pom.xml
```xml
	<dependency>
	    <groupId>com.github.APIJSON</groupId>
	    <artifactId>apijson-influxdb</artifactId>
	    <version>LATEST</version>
	</dependency>
```

<br />

https://github.com/APIJSON/APIJSON-Demo/blob/master/APIJSON-Java-Server/APIJSONBoot-MultiDataSource/pom.xml

<br />
<br />

### Gradle
#### 1. 在项目根目录 build.gradle 中最后添加 JitPack 仓库
#### 1. Add the JitPack repository in your root build.gradle at the end of repositories
```gradle
	allprojects {
		repositories {
			maven { url 'https://jitpack.io' }
		}
	}
```
<br />

#### 2. 在项目某个 module 目录(例如 `app`) build.gradle 中添加 apijson-influxdb 依赖
#### 2. Add the apijson-influxdb dependency in one of your modules(such as `app`)
```gradle
	dependencies {
	        implementation 'com.github.APIJSON:apijson-influxdb:latest'
	}
```

<br />
<br />
<br />

## 使用
## Usage

在你项目继承 AbstractSQLExecutor 的子类重写方法 execute <br/>
Override execute in your SQLExecutor extends AbstractSQLExecutor

```java
        @Override
        public JSONObject execute(@NotNull SQLConfig<Long> config, boolean unknownType) throws Exception {
            if (config.isInfluxDB()) {
                return InfluxdbUtil.execute(config, null, unknownType);
            }
   
            return super.execute(config, unknownType);
        }
```

<br/>
在你项目继承 AbstractSQLConfig 的子类重写方法 execute <br/>
Override execute in your SQLConfig extends AbstractSQLConfig

```java
	@Override
	public String getSchema() {
		return InfluxDBUtil.getSchema(super.getSchema(), DEFAULT_SCHEMA, isInfluxDB());
	}

	@Override
	public String getSQLSchema() {
		return InfluxDBUtil.getSQLSchema(super.getSQLSchema(), isInfluxDB());
	}
```

#### 见 [InfluxDBUtil](/src/main/java/apijson/influxdb/InfluxDBUtil.java) 的注释及 [APIJSONBoot-MultiDataSource](https://github.com/APIJSON/APIJSON-Demo/blob/master/APIJSON-Java-Server/APIJSONBoot-MultiDataSource) 的 [DemoSQLExecutor](https://github.com/APIJSON/APIJSON-Demo/blob/master/APIJSON-Java-Server/APIJSONBoot-MultiDataSource/src/main/java/apijson/demo/DemoSQLExecutor.java) <br />

#### See document in [InfluxDBUtil](/src/main/java/apijson/influxdb/InfluxDBUtil.java) and [DemoSQLExecutor](https://github.com/APIJSON/APIJSON-Demo/blob/master/APIJSON-Java-Server/APIJSONBoot-MultiDataSource/src/main/java/apijson/demo/DemoSQLExecutor.java) in [APIJSONBoot-MultiDataSource](https://github.com/APIJSON/APIJSON-Demo/blob/master/APIJSON-Java-Server/APIJSONBoot-MultiDataSource)

<br />
<br />
<br />

有问题可以去 Tencent/APIJSON 提 issue <br />
https://github.com/Tencent/APIJSON/issues/36

<br /><br />

#### 点右上角 ⭐Star 支持一下，谢谢 ^_^
#### Please ⭐Star this project ^_^
https://github.com/APIJSON/apijson-influxdb
