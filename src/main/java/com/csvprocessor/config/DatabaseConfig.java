package com.csvprocessor.config;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Duration;
import java.util.*;

/**
 * Configuración de base de datos R2DBC para PostgreSQL
 * Optimizada para procesamiento paralelo de alto rendimiento de archivos Excel
 * Estructura: NO.DOCUMENTO, PRIMERNOMBRE, SEGUNDONOMBRE, PRIMERAPELLIDO, SEGUNDOAPELLIDO,
 * CODIGODANE, DEPARTAMENTO, MUNICIPIO, VALORGIRO, CODIGOPROGRAMA, PROGRAMA, FECHADECOLOCACIÓN
 */
@Configuration
@EnableR2dbcRepositories(basePackages = "com.csvprocessor.repository")
@EnableTransactionManagement
public class DatabaseConfig extends AbstractR2dbcConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);

    @Value("${spring.r2dbc.host:localhost}")
    private String host;

    @Value("${spring.r2dbc.port:5432}")
    private int port;

    @Value("${spring.r2dbc.database:csvdb}")
    private String database;

    @Value("${spring.r2dbc.username:postgres}")
    private String username;

    @Value("${spring.r2dbc.password:password}")
    private String password;

    // Configuración del pool de conexiones optimizada para Excel
    @Value("${spring.r2dbc.pool.initial-size:15}")
    private int initialSize;

    @Value("${spring.r2dbc.pool.max-size:40}")
    private int maxSize;

    @Value("${spring.r2dbc.pool.max-idle-time:PT30M}")
    private Duration maxIdleTime;

    @Value("${spring.r2dbc.pool.max-acquire-time:PT60S}")
    private Duration maxAcquireTime;

    @Value("${spring.r2dbc.pool.max-create-connection-time:PT30S}")
    private Duration maxCreateConnectionTime;

    // Configuración de timeouts para operaciones Excel
    @Value("${spring.r2dbc.connect-timeout:PT15S}")
    private Duration connectTimeout;

    // Configuración de esquema
    @Value("${spring.r2dbc.schema.initialize:true}")
    private boolean initializeSchema;

    @Value("${spring.r2dbc.schema.continue-on-error:false}")
    private boolean continueOnError;

    /**
     * Configuración principal del ConnectionFactory para PostgreSQL básico
     */
    @Override
    @Bean
    public ConnectionFactory connectionFactory() {
        logger.info("Configurando ConnectionFactory para PostgreSQL - Host: {}:{}, Database: {}",
                host, port, database);

        PostgresqlConnectionConfiguration config = PostgresqlConnectionConfiguration.builder()
                .host(host)
                .port(port)
                .database(database)
                .username(username)
                .password(password)
                // Timeout de conexión
                .connectTimeout(connectTimeout)
                // Configuraciones de aplicación
                .applicationName("excel-processor")
                .schema("public")
                .build();

        return new PostgresqlConnectionFactory(config);
    }

    /**
     * Cliente de base de datos configurado para operaciones reactivas
     */
    @Bean
    public DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
        return DatabaseClient.builder()
                .connectionFactory(connectionFactory)
                .namedParameters(true)
                .build();
    }

    /**
     * Administrador de transacciones reactivas
     */
    @Bean
    public ReactiveTransactionManager transactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }

    /**
     * Inicializador del esquema de base de datos
     */
    @Bean
    public ConnectionFactoryInitializer initializer(ConnectionFactory connectionFactory) {
        ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
        initializer.setConnectionFactory(connectionFactory);

        if (initializeSchema) {
            logger.info("Inicializando esquema de base de datos");

            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.addScript(new ClassPathResource("schema.sql"));
            populator.setContinueOnError(continueOnError);

            initializer.setDatabasePopulator(populator);
        }

        return initializer;
    }

    /**
     * Configuración personalizada de R2DBC para operaciones batch Excel
     */
    @Bean
    public ExcelBatchDatabaseConfig excelBatchDatabaseConfig() {
        return ExcelBatchDatabaseConfig.builder()
                .batchSize(1500)
                .maxBatchWaitTime(Duration.ofMillis(80))
                .parallelBatches(12)
                .retryAttempts(3)
                .retryDelay(Duration.ofMillis(400))
                .build();
    }

    /**
     * Configuración específica para operaciones batch Excel
     */
    public static class ExcelBatchDatabaseConfig {
        private final int batchSize;
        private final Duration maxBatchWaitTime;
        private final int parallelBatches;
        private final int retryAttempts;
        private final Duration retryDelay;

        private ExcelBatchDatabaseConfig(Builder builder) {
            this.batchSize = builder.batchSize;
            this.maxBatchWaitTime = builder.maxBatchWaitTime;
            this.parallelBatches = builder.parallelBatches;
            this.retryAttempts = builder.retryAttempts;
            this.retryDelay = builder.retryDelay;
        }

        public static Builder builder() {
            return new Builder();
        }

        // Getters
        public int getBatchSize() { return batchSize; }
        public Duration getMaxBatchWaitTime() { return maxBatchWaitTime; }
        public int getParallelBatches() { return parallelBatches; }
        public int getRetryAttempts() { return retryAttempts; }
        public Duration getRetryDelay() { return retryDelay; }

        public static class Builder {
            private int batchSize = 1500;
            private Duration maxBatchWaitTime = Duration.ofMillis(80);
            private int parallelBatches = 12;
            private int retryAttempts = 3;
            private Duration retryDelay = Duration.ofMillis(400);

            public Builder batchSize(int batchSize) {
                this.batchSize = batchSize;
                return this;
            }

            public Builder maxBatchWaitTime(Duration maxBatchWaitTime) {
                this.maxBatchWaitTime = maxBatchWaitTime;
                return this;
            }

            public Builder parallelBatches(int parallelBatches) {
                this.parallelBatches = parallelBatches;
                return this;
            }

            public Builder retryAttempts(int retryAttempts) {
                this.retryAttempts = retryAttempts;
                return this;
            }

            public Builder retryDelay(Duration retryDelay) {
                this.retryDelay = retryDelay;
                return this;
            }

            public ExcelBatchDatabaseConfig build() {
                return new ExcelBatchDatabaseConfig(this);
            }
        }
    }

    /**
     * Health check para la conexión de base de datos Excel
     */
    @Bean
    public ExcelDatabaseHealthCheck excelDatabaseHealthCheck(DatabaseClient databaseClient) {
        return new ExcelDatabaseHealthCheck(databaseClient);
    }

    /**
     * Clase para verificar el estado de la base de datos Excel
     */
    public static class ExcelDatabaseHealthCheck {
        private static final Logger logger = LoggerFactory.getLogger(ExcelDatabaseHealthCheck.class);
        private final DatabaseClient databaseClient;

        public ExcelDatabaseHealthCheck(DatabaseClient databaseClient) {
            this.databaseClient = databaseClient;
        }

        public reactor.core.publisher.Mono<Boolean> checkHealth() {
            return databaseClient
                    .sql("SELECT 1")
                    .fetch()
                    .first()
                    .map(result -> true)
                    .doOnSuccess(result -> logger.debug("Excel database health check successful"))
                    .doOnError(error -> logger.error("Excel database health check failed", error))
                    .onErrorReturn(false)
                    .timeout(Duration.ofSeconds(10));
        }

        public reactor.core.publisher.Mono<Boolean> checkTableExists(String tableName) {
            String sql = "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = :tableName";
            return databaseClient
                    .sql(sql)
                    .bind("tableName", tableName)
                    .fetch()
                    .first()
                    .map(result -> {
                        Number count = (Number) result.get("count");
                        return count != null && count.intValue() > 0;
                    })
                    .doOnSuccess(exists -> logger.debug("Table {} exists: {}", tableName, exists))
                    .doOnError(error -> logger.error("Error checking table existence: {}", tableName, error))
                    .onErrorReturn(false);
        }

        public reactor.core.publisher.Mono<Long> getTableRowCount(String tableName) {
            String sql = "SELECT COUNT(*) as count FROM " + tableName;
            return databaseClient
                    .sql(sql)
                    .fetch()
                    .first()
                    .map(result -> {
                        Number count = (Number) result.get("count");
                        return count != null ? count.longValue() : 0L;
                    })
                    .doOnSuccess(count -> logger.debug("Table {} has {} rows", tableName, count))
                    .doOnError(error -> logger.error("Error getting row count for table: {}", tableName, error))
                    .onErrorReturn(0L);
        }

        public reactor.core.publisher.Mono<Boolean> testBatchInsert() {
            String testTable = "excel_records";
            return checkTableExists(testTable)
                    .flatMap(exists -> {
                        if (!exists) {
                            logger.warn("Test table {} does not exist, skipping batch insert test", testTable);
                            return reactor.core.publisher.Mono.just(false);
                        }

                        // Perform a simple test insert/delete
                        String insertSql = "INSERT INTO " + testTable +
                                " (numero_documento, primer_nombre, primer_apellido, codigo_dane, " +
                                "departamento, municipio, valor_giro, codigo_programa, programa, fecha_colocacion) " +
                                "VALUES ('99999999', 'TEST', 'TEST', '00000', 'TEST', 'TEST', 1000, 'TEST', 'TEST', CURRENT_DATE)";
                        String deleteSql = "DELETE FROM " + testTable + " WHERE numero_documento = '99999999'";

                        return databaseClient.sql(insertSql)
                                .fetch()
                                .rowsUpdated()
                                .flatMap(inserted -> databaseClient.sql(deleteSql).fetch().rowsUpdated())
                                .map(deleted -> true)
                                .doOnSuccess(result -> logger.debug("Batch insert test successful"))
                                .doOnError(error -> logger.error("Batch insert test failed", error))
                                .onErrorReturn(false);
                    });
        }
    }

    /**
     * Configuración específica para el esquema Excel
     */
    @Bean
    public ExcelSchemaConfig excelSchemaConfig() {
        return new ExcelSchemaConfig();
    }

    /**
     * Configuración del esquema de base de datos para datos de Excel
     */
    public static class ExcelSchemaConfig {
        private static final Logger logger = LoggerFactory.getLogger(ExcelSchemaConfig.class);

        // Configuración de columnas para la tabla excel_records
        public static final String TABLE_NAME = "excel_records";

        // Mapeo de columnas Excel a base de datos
        public static final String COL_NUMERO_DOCUMENTO = "numero_documento";
        public static final String COL_PRIMER_NOMBRE = "primer_nombre";
        public static final String COL_SEGUNDO_NOMBRE = "segundo_nombre";
        public static final String COL_PRIMER_APELLIDO = "primer_apellido";
        public static final String COL_SEGUNDO_APELLIDO = "segundo_apellido";
        public static final String COL_CODIGO_DANE = "codigo_dane";
        public static final String COL_DEPARTAMENTO = "departamento";
        public static final String COL_MUNICIPIO = "municipio";
        public static final String COL_VALOR_GIRO = "valor_giro";
        public static final String COL_CODIGO_PROGRAMA = "codigo_programa";
        public static final String COL_PROGRAMA = "programa";
        public static final String COL_FECHA_COLOCACION = "fecha_colocacion";

        // SQL optimizado para inserción batch
        public String getBatchInsertSql() {
            return "INSERT INTO " + TABLE_NAME + " (" +
                    COL_NUMERO_DOCUMENTO + ", " +
                    COL_PRIMER_NOMBRE + ", " +
                    COL_SEGUNDO_NOMBRE + ", " +
                    COL_PRIMER_APELLIDO + ", " +
                    COL_SEGUNDO_APELLIDO + ", " +
                    COL_CODIGO_DANE + ", " +
                    COL_DEPARTAMENTO + ", " +
                    COL_MUNICIPIO + ", " +
                    COL_VALOR_GIRO + ", " +
                    COL_CODIGO_PROGRAMA + ", " +
                    COL_PROGRAMA + ", " +
                    COL_FECHA_COLOCACION +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        }

        // SQL para validación de duplicados por documento
        public String getDuplicateCheckSql() {
            return "SELECT COUNT(*) as count FROM " + TABLE_NAME +
                    " WHERE " + COL_NUMERO_DOCUMENTO + " = ?";
        }

        // SQL para estadísticas de procesamiento
        public String getProcessingStatsSql() {
            return "SELECT " +
                    "COUNT(*) as total_records, " +
                    "COUNT(DISTINCT " + COL_NUMERO_DOCUMENTO + ") as unique_documents, " +
                    "COUNT(DISTINCT " + COL_CODIGO_DANE + ") as unique_municipalities, " +
                    "SUM(" + COL_VALOR_GIRO + ") as total_amount, " +
                    "AVG(" + COL_VALOR_GIRO + ") as average_amount, " +
                    "MIN(" + COL_FECHA_COLOCACION + ") as earliest_date, " +
                    "MAX(" + COL_FECHA_COLOCACION + ") as latest_date " +
                    "FROM " + TABLE_NAME;
        }

        // Configuración de índices para optimización
        public List<String> getOptimizationIndexes() {
            return Arrays.asList(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excel_documento ON " + TABLE_NAME + "(" + COL_NUMERO_DOCUMENTO + ")",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excel_dane ON " + TABLE_NAME + "(" + COL_CODIGO_DANE + ")",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excel_fecha ON " + TABLE_NAME + "(" + COL_FECHA_COLOCACION + ")",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excel_programa ON " + TABLE_NAME + "(" + COL_CODIGO_PROGRAMA + ")",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excel_valor ON " + TABLE_NAME + "(" + COL_VALOR_GIRO + ")",
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_excel_ubicacion ON " + TABLE_NAME + "(" + COL_DEPARTAMENTO + ", " + COL_MUNICIPIO + ")"
            );
        }

        // Mapeo de columnas Excel (headers) a columnas de BD
        public Map<String, String> getExcelToDbColumnMapping() {
            Map<String, String> mapping = new HashMap<>();
            mapping.put("NO.DEDOCUMENTO", COL_NUMERO_DOCUMENTO);
            mapping.put("PRIMERNOMBRE", COL_PRIMER_NOMBRE);
            mapping.put("SEGUNDONOMBRE", COL_SEGUNDO_NOMBRE);
            mapping.put("PRIMERAPELLIDO", COL_PRIMER_APELLIDO);
            mapping.put("SEGUNDOAPELLIDO", COL_SEGUNDO_APELLIDO);
            mapping.put("CODIGODANE", COL_CODIGO_DANE);
            mapping.put("DEPARTAMENTO", COL_DEPARTAMENTO);
            mapping.put("MUNICIPIO", COL_MUNICIPIO);
            mapping.put("VALORGIRO", COL_VALOR_GIRO);
            mapping.put("CODIGOPROGRAMA", COL_CODIGO_PROGRAMA);
            mapping.put("PROGRAMA", COL_PROGRAMA);
            mapping.put("FECHADECOLOCACIÓN", COL_FECHA_COLOCACION);
            return mapping;
        }

        // Validación de estructura de Excel
        public boolean validateExcelHeaders(List<String> headers) {
            Set<String> requiredHeaders = getExcelToDbColumnMapping().keySet();
            Set<String> providedHeaders = new HashSet<>(headers);

            boolean isValid = providedHeaders.containsAll(requiredHeaders);

            if (!isValid) {
                Set<String> missingHeaders = new HashSet<>(requiredHeaders);
                missingHeaders.removeAll(providedHeaders);
                logger.error("Missing required Excel headers: {}", missingHeaders);
            }

            return isValid;
        }
    }
}