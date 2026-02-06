plugins {
    id("java")
}

group = "co.demo.elastic"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.17.0")
    implementation("dev.langchain4j:langchain4j-elasticsearch:1.11.0-beta19")
    implementation("dev.langchain4j:langchain4j-ollama:1.11.0")
}

tasks.test {
    useJUnitPlatform()
}
