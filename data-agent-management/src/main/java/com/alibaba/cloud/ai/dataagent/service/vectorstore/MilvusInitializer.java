package com.alibaba.cloud.ai.dataagent.service.vectorstore;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.milvus.autoconfigure.MilvusVectorStoreProperties;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Milvus 初始化
 *
 * @author Yangyf
 * @date 2023/11/02
 */
@Component
@RequiredArgsConstructor
public class MilvusInitializer implements ApplicationRunner {

	private final MilvusServiceClient milvusClient;

	private final MilvusVectorStoreProperties storeProperties;

	private final EmbeddingModel embeddingModel;

	@Override
	public void run(ApplicationArguments args) {

		String collectionName = storeProperties.getCollectionName();
		String databaseName = storeProperties.getDatabaseName();

		// 1. collection 是否存在
		boolean exists = milvusClient
			.hasCollection(HasCollectionParam.newBuilder()
				.withDatabaseName(databaseName)
				.withCollectionName(collectionName)
				.build())
			.getData();

		if (!exists) {
			createCollection(collectionName, databaseName);
		}

		// 2. 索引是否存在
		if (!hasIndex(collectionName, databaseName)) {
			createIndex(collectionName, databaseName);
		}
	}

	private void createCollection(String collectionName, String databaseName) {

		int dimension = embeddingModel.dimensions();

		CreateCollectionParam param = CreateCollectionParam.newBuilder()
			.withDatabaseName(databaseName)
			.withCollectionName(collectionName)
			.withDescription("auto created by spring-ai")
			.withShardsNum(2)
			.addFieldType(FieldType.newBuilder()
				.withName("doc_id")
				.withDataType(DataType.VarChar)
				.withPrimaryKey(true)
				.withMaxLength(256)
				.build())
			.addFieldType(FieldType.newBuilder()
				.withName("content")
				.withDataType(DataType.VarChar)
				.withMaxLength(8192)
				.build())
			.addFieldType(FieldType.newBuilder()
				.withName("embedding")
				.withDataType(DataType.FloatVector)
				.withDimension(dimension)
				.build())
			.addFieldType(FieldType.newBuilder().withName("metadata").withDataType(DataType.JSON).build())
			.build();

		milvusClient.createCollection(param);
	}

	private boolean hasIndex(String collectionName, String databaseName) {
		try {
			return null != milvusClient
				.describeIndex(DescribeIndexParam.newBuilder()
					.withDatabaseName(databaseName)
					.withCollectionName(collectionName)
					.build())
				.getData();
		}
		catch (Exception e) {
			return false;
		}
	}

	private void createIndex(String collectionName, String databaseName) {
		milvusClient.createIndex(CreateIndexParam.newBuilder()
			.withDatabaseName(databaseName)
			.withCollectionName(collectionName)
			.withFieldName("embedding")
			.withIndexType(IndexType.FLAT) // 索引类型HNSW
			.withMetricType(MetricType.COSINE)
			.withExtraParam("{\"M\":16,\"efConstruction\":100}")
			.build());

		// 显式加载 Collection
		milvusClient.loadCollection(LoadCollectionParam.newBuilder()
			.withDatabaseName(databaseName)
			.withCollectionName(collectionName)
			.build());
	}

}
