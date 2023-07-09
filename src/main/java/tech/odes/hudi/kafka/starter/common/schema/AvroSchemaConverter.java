package tech.odes.hudi.kafka.starter.common.schema;

import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.hudi.org.apache.avro.LogicalType;
import org.apache.hudi.org.apache.avro.LogicalTypes;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.org.apache.avro.SchemaBuilder;
import org.apache.hudi.org.apache.avro.LogicalTypes.Decimal;
import org.apache.hudi.org.apache.avro.Schema.Field;
import org.apache.hudi.org.apache.avro.Schema.Type;
import org.apache.hudi.org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.hudi.org.apache.avro.SchemaBuilder.GenericDefault;


/**
 * Converts an Avro schema into Flink's type information. It uses {@link org.apache.flink.api.java.typeutils.RowTypeInfo} for
 * representing objects and converts Avro types into types that are compatible with Flink's Table &
 * SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime classes
 * {@link org.apache.flink.formats.avro.AvroRowDeserializationSchema} and {@link org.apache.flink.formats.avro.AvroRowSerializationSchema}.
 *
 * <p>NOTE: reference from Flink release 1.12.0, should remove when Flink version upgrade to that.
 */
@Deprecated
public class AvroSchemaConverter {

    /**
     * Converts an Avro schema {@code schema} into a nested row structure with deterministic field order and
     * data types that are compatible with Flink's Table & SQL API.
     *
     * @param schema Avro schema definition
     * @return data type matching the schema
     */
    public static DataType convertToDataType(Schema schema) {
        Decimal decimalType;
        switch(schema.getType()) {
            case RECORD:
                List<Field> schemaFields = schema.getFields();
                org.apache.flink.table.api.DataTypes.Field[] fields =
                        new org.apache.flink.table.api.DataTypes.Field[schemaFields.size()];

                for(int i = 0; i < schemaFields.size(); ++i) {
                    Field field = schemaFields.get(i);
                    fields[i] = DataTypes.FIELD(field.name(), convertToDataType(field.schema()));
                }

                return DataTypes.ROW(fields).notNull();
            case ENUM:
                return DataTypes.STRING().notNull();
            case ARRAY:
                return DataTypes.ARRAY(convertToDataType(schema.getElementType())).notNull();
            case MAP:
                return DataTypes.MAP((DataType)DataTypes.STRING().notNull(), convertToDataType(schema.getValueType())).notNull();
            case UNION:
                Schema actualSchema;
                boolean nullable;
                if (schema.getTypes().size() == 2 && (
                        schema.getTypes().get(0)).getType() == Type.NULL) {
                    actualSchema = schema.getTypes().get(1);
                    nullable = true;
                } else if (schema.getTypes().size() == 2 &&
                        (schema.getTypes().get(1)).getType() == Type.NULL) {
                    actualSchema = schema.getTypes().get(0);
                    nullable = true;
                } else {
                    if (schema.getTypes().size() != 1) {
                        return new AtomicDataType(new TypeInformationRawType(false, Types.GENERIC(Object.class)));
                    }
                    actualSchema = schema.getTypes().get(0);
                    nullable = false;
                }

                DataType converted = convertToDataType(actualSchema);
                return nullable ? converted.nullable() : converted;
            case FIXED:
                if (schema.getLogicalType() instanceof Decimal) {
                    decimalType = (Decimal)schema.getLogicalType();
                    return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale()).notNull();
                }

                return DataTypes.VARBINARY(schema.getFixedSize()).notNull();
            case STRING:
                return DataTypes.STRING().notNull();
            case BYTES:
                if (schema.getLogicalType() instanceof Decimal) {
                    decimalType = (Decimal)schema.getLogicalType();
                    return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale()).notNull();
                }

                return DataTypes.BYTES().notNull();
            case INT:
                LogicalType logicalType = schema.getLogicalType();
                if (logicalType == LogicalTypes.date()) {
                    return DataTypes.DATE().notNull();
                } else {
                    if (logicalType == LogicalTypes.timeMillis()) {
                        return DataTypes.TIME(3).notNull();
                    }

                    return DataTypes.INT().notNull();
                }
            case LONG:
                if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
                    return DataTypes.TIMESTAMP(3).notNull();
                } else if (schema.getLogicalType() == LogicalTypes.timestampMicros()) {
                    // avro not support timestamp precision more than 3 and cast to bigint
                    // return DataTypes.TIMESTAMP(6).notNull();
                    return DataTypes.BIGINT().notNull();
                } else if (schema.getLogicalType() == LogicalTypes.timeMillis()) {
                    return DataTypes.TIME(3).notNull();
                } else {
                    if (schema.getLogicalType() == LogicalTypes.timeMicros()) {
                        // avro not support timestamp precision more than 3 and cast to bigint
                        // return DataTypes.TIME(6).notNull();
                        return DataTypes.BIGINT().notNull();
                    }

                    return DataTypes.BIGINT().notNull();
                }
            case FLOAT:
                return DataTypes.FLOAT().notNull();
            case DOUBLE:
                return DataTypes.DOUBLE().notNull();
            case BOOLEAN:
                return DataTypes.BOOLEAN().notNull();
            case NULL:
                return DataTypes.NULL();
            default:
                throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
        }
    }

    /**
     * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
     *
     * <p>The "{rowName}_" is used as the nested row type name prefix in order to generate the right
     * schema. Nested record type that only differs with type name is still compatible.
     *
     * @param logicalType logical type
     * @param rowName the record name
     * @return Avro's {@link Schema} matching this logical type.
     */
    public static Schema convertToSchema(org.apache.flink.table.types.logical.LogicalType logicalType, String rowName) {
        boolean nullable = logicalType.isNullable();
        int precision;
        switch(logicalType.getTypeRoot()) {
            case NULL:
                return SchemaBuilder.builder().nullType();
            case BOOLEAN:
                Schema bool = SchemaBuilder.builder().booleanType();
                return nullable ? nullableSchema(bool) : bool;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                Schema integer = SchemaBuilder.builder().intType();
                return nullable ? nullableSchema(integer) : integer;
            case BIGINT:
                Schema bigint = SchemaBuilder.builder().longType();
                return nullable ? nullableSchema(bigint) : bigint;
            case FLOAT:
                Schema f = SchemaBuilder.builder().floatType();
                return nullable ? nullableSchema(f) : f;
            case DOUBLE:
                Schema d = SchemaBuilder.builder().doubleType();
                return nullable ? nullableSchema(d) : d;
            case CHAR:
            case VARCHAR:
                Schema str = SchemaBuilder.builder().stringType();
                return nullable ? nullableSchema(str) : str;
            case BINARY:
            case VARBINARY:
                Schema binary = SchemaBuilder.builder().bytesType();
                return nullable ? nullableSchema(binary) : binary;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType)logicalType;
                precision = timestampType.getPrecision();
                if (precision <= 3) {
                    LogicalType avroLogicalType = LogicalTypes.timestampMillis();
                    Schema timestamp = avroLogicalType.addToSchema(SchemaBuilder.builder().longType());
                    return nullable ? nullableSchema(timestamp) : timestamp;
                }

                throw new IllegalArgumentException("Avro does not support TIMESTAMP type with precision: " +
                        precision + ", it only supports precision less than 3.");
            case DATE:
                Schema date = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
                return nullable ? nullableSchema(date) : date;
            case TIME_WITHOUT_TIME_ZONE:
                precision = ((TimeType)logicalType).getPrecision();
                if (precision > 3) {
                    throw new IllegalArgumentException("Avro does not support TIME type with precision: " +
                            precision + ", it only supports precision less than 3.");
                }

                Schema time = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
                return nullable ? nullableSchema(time) : time;
            case DECIMAL:
                DecimalType decimalType = (DecimalType)logicalType;
                Schema decimal = LogicalTypes.decimal(decimalType.getPrecision(), decimalType.getScale())
                        .addToSchema(SchemaBuilder.builder().bytesType());
                return nullable ? nullableSchema(decimal) : decimal;
            case ROW:
                RowType rowType = (RowType)logicalType;
                List<String> fieldNames = rowType.getFieldNames();
                FieldAssembler<Schema> builder = SchemaBuilder.builder().record(rowName).fields();

                for(int i = 0; i < rowType.getFieldCount(); ++i) {
                    String fieldName = fieldNames.get(i);
                    org.apache.flink.table.types.logical.LogicalType fieldType = rowType.getTypeAt(i);
                    GenericDefault<Schema> fieldBuilder = builder.name(fieldName)
                            .type(convertToSchema(fieldType, rowName + "_" + fieldName));
                    if (fieldType.isNullable()) {
                        builder = fieldBuilder.withDefault(null);
                    } else {
                        builder = fieldBuilder.noDefault();
                    }
                }

                Schema record = builder.endRecord();
                return nullable ? nullableSchema(record) : record;
            case MULTISET:
            case MAP:
                Schema map = SchemaBuilder.builder().map().values(
                        convertToSchema(extractValueTypeToAvroMap(logicalType), rowName));
                return nullable ? nullableSchema(map) : map;
            case ARRAY:
                ArrayType arrayType = (ArrayType)logicalType;
                Schema array = SchemaBuilder.builder().array().items(convertToSchema(arrayType.getElementType(), rowName));
                return nullable ? nullableSchema(array) : array;
            case RAW:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            default:
                throw new UnsupportedOperationException("Unsupported to derive Schema for type: " + logicalType);
        }
    }

    private static org.apache.flink.table.types.logical.LogicalType extractValueTypeToAvroMap(
            org.apache.flink.table.types.logical.LogicalType type) {
        org.apache.flink.table.types.logical.LogicalType keyType;
        Object valueType;
        if (type instanceof MapType) {
            MapType mapType = (MapType)type;
            keyType = mapType.getKeyType();
            valueType = mapType.getValueType();
        } else {
            MultisetType multisetType = (MultisetType)type;
            keyType = multisetType.getElementType();
            valueType = new IntType();
        }

        if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException("Avro format doesn't support non-string as key type of map. " +
                    "The key type is: " + keyType.asSummaryString());
        } else {
            return (org.apache.flink.table.types.logical.LogicalType)valueType;
        }
    }

    /** Returns schema with nullable true. */
    private static Schema nullableSchema(Schema schema) {
        return schema.isNullable()
                ? schema
                : Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
    }
}
