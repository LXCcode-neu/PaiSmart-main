package com.yizhaoqi.smartpai.service;

import com.yizhaoqi.smartpai.model.ChunkInfo;
import com.yizhaoqi.smartpai.model.FileUpload;
import com.yizhaoqi.smartpai.repository.ChunkInfoRepository;
import com.yizhaoqi.smartpai.repository.FileUploadRepository;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.messages.Item;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 运行前请先启动本地 Redis 和 MinIO：
 * redis: localhost:6379
 * minio: localhost:9000
 *
 * 默认不开启，避免普通 test 被 1GB 基准拖慢。
 * 运行方式：
 * PowerShell:
 * $env:RUN_UPLOAD_BENCHMARK='true'; mvn -Dtest=UploadServiceBenchmarkTest test
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(named = "RUN_UPLOAD_BENCHMARK", matches = "true")
class UploadServiceBenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(UploadServiceBenchmarkTest.class);

    private static final String BUCKET = "uploads";
    private static final String USER_ID = "benchmark-user";
    private static final String FILE_MD5 = "0123456789abcdef0123456789abcdef";
    private static final String FILE_NAME = "benchmark-1g.bin";
    private static final String ORG_TAG = "benchmark";
    private static final long ONE_GIB = 1024L * 1024 * 1024;
    private static final int CHUNK_SIZE = 5 * 1024 * 1024;
    private static final int TOTAL_CHUNKS = (int) Math.ceil((double) ONE_GIB / CHUNK_SIZE);
    private static final int WARMUP_ROUNDS = 5;
    private static final int MEASURE_ROUNDS = 30;

    private RedisTemplate<String, Object> redisTemplate;
    private MinioClient minioClient;
    private UploadService uploadService;

    private final Map<String, FileUpload> fileUploads = new ConcurrentHashMap<>();
    private final Map<String, List<ChunkInfo>> chunkInfos = new ConcurrentHashMap<>();

    @BeforeAll
    void beforeAll() throws Exception {
        redisTemplate = buildRedisTemplate();
        minioClient = MinioClient.builder()
                .endpoint("http://localhost:9000")
                .credentials("minioadmin", "minioadmin")
                .build();

        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        fileUploads.clear();
        chunkInfos.clear();
        cleanupStorage();
        redisTemplate.delete(redisKey());
        uploadService = buildUploadService();
    }

    @Test
    void benchmarkUploadedChunksLookupForOneGiBFile() {
        seedFileUploadRecord();
        for (int i = 0; i < TOTAL_CHUNKS; i++) {
            redisTemplate.opsForValue().setBit(redisKey(), i, true);
        }

        List<Integer> oldResult = oldGetUploadedChunks(FILE_MD5, USER_ID);
        List<Integer> newResult = uploadService.getUploadedChunks(FILE_MD5, USER_ID);
        assertEquals(TOTAL_CHUNKS, oldResult.size());
        assertEquals(TOTAL_CHUNKS, newResult.size());

        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            oldGetUploadedChunks(FILE_MD5, USER_ID);
            uploadService.getUploadedChunks(FILE_MD5, USER_ID);
        }

        long oldTotalNs = 0;
        long newTotalNs = 0;
        for (int i = 0; i < MEASURE_ROUNDS; i++) {
            long oldStart = System.nanoTime();
            oldGetUploadedChunks(FILE_MD5, USER_ID);
            oldTotalNs += System.nanoTime() - oldStart;

            long newStart = System.nanoTime();
            uploadService.getUploadedChunks(FILE_MD5, USER_ID);
            newTotalNs += System.nanoTime() - newStart;
        }

        double oldAvgMs = nsToMs(oldTotalNs / (double) MEASURE_ROUNDS);
        double newAvgMs = nsToMs(newTotalNs / (double) MEASURE_ROUNDS);
        double speedup = oldAvgMs / newAvgMs;

        logger.info("==== Redis BitMap 查询基准 ====");
        logger.info("文件大小: {} bytes, 分片大小: {} bytes, 分片数: {}", ONE_GIB, CHUNK_SIZE, TOTAL_CHUNKS);
        logger.info("优化前平均耗时: {} ms", round2(oldAvgMs));
        logger.info("优化后平均耗时: {} ms", round2(newAvgMs));
        logger.info("提速倍数: {}x", round2(speedup));

        assertTrue(speedup > 1.5, "优化后应明显快于逐个 getBit 查询");
    }

    @Test
    void benchmarkOneGiBUploadMergeAndAsyncProxyLatency() throws Exception {
        byte[] commonChunk = new byte[CHUNK_SIZE];
        byte[] lastChunk = new byte[(int) (ONE_GIB - (long) (TOTAL_CHUNKS - 1) * CHUNK_SIZE)];
        Arrays.fill(commonChunk, (byte) 'a');
        Arrays.fill(lastChunk, (byte) 'b');

        long uploadStart = System.nanoTime();
        for (int i = 0; i < TOTAL_CHUNKS; i++) {
            byte[] payload = i == TOTAL_CHUNKS - 1 ? lastChunk : commonChunk;
            MockMultipartFile chunkFile = new MockMultipartFile(
                    "file",
                    FILE_NAME + ".part" + i,
                    "application/octet-stream",
                    payload
            );
            uploadService.uploadChunk(FILE_MD5, i, ONE_GIB, FILE_NAME, chunkFile, ORG_TAG, false, USER_ID);
        }
        long uploadNs = System.nanoTime() - uploadStart;

        long mergeStart = System.nanoTime();
        String presignedUrl = uploadService.mergeChunks(FILE_MD5, FILE_NAME, USER_ID);
        long mergeNs = System.nanoTime() - mergeStart;

        assertNotNull(presignedUrl);
        minioClient.statObject(StatObjectArgs.builder().bucket(BUCKET).object("merged/" + FILE_MD5).build());

        long preprocessProxyStart = System.nanoTime();
        long mergedBytes = readMergedObjectBytes();
        long preprocessProxyNs = System.nanoTime() - preprocessProxyStart;

        assertEquals(ONE_GIB, mergedBytes);

        double uploadMs = nsToMs(uploadNs);
        double mergeMs = nsToMs(mergeNs);
        double preprocessProxyMs = nsToMs(preprocessProxyNs);
        double currentUserVisibleMs = uploadMs + mergeMs;
        double syncBlockingProxyMs = uploadMs + mergeMs + preprocessProxyMs;
        double savedMs = syncBlockingProxyMs - currentUserVisibleMs;
        double reductionPct = savedMs / syncBlockingProxyMs * 100;

        logger.info("==== 1GB 上传与异步化收益基准 ====");
        logger.info("1GB 分片上传总耗时: {} ms", round2(uploadMs));
        logger.info("合并并返回预签名 URL 耗时: {} ms", round2(mergeMs));
        logger.info("后台预处理下界代理耗时（仅读取完整 1GB 合并文件）: {} ms", round2(preprocessProxyMs));
        logger.info("当前用户可感知完成时间（上传 + 合并返回）: {} ms", round2(currentUserVisibleMs));
        logger.info("若同步阻塞等待预处理下界完成: {} ms", round2(syncBlockingProxyMs));
        logger.info("异步化至少节省: {} ms, 降幅: {}%", round2(savedMs), round2(reductionPct));
    }

    private UploadService buildUploadService() {
        UploadService service = new UploadService();
        FileUploadRepository fileUploadRepository = Mockito.mock(FileUploadRepository.class);
        ChunkInfoRepository chunkInfoRepository = Mockito.mock(ChunkInfoRepository.class);

        when(fileUploadRepository.findByFileMd5AndUserId(anyString(), anyString())).thenAnswer(invocation -> {
            String fileMd5 = invocation.getArgument(0);
            String userId = invocation.getArgument(1);
            return Optional.ofNullable(fileUploads.get(fileKey(fileMd5, userId)));
        });

        when(fileUploadRepository.save(any(FileUpload.class))).thenAnswer(invocation -> {
            FileUpload upload = invocation.getArgument(0);
            fileUploads.put(fileKey(upload.getFileMd5(), upload.getUserId()), upload);
            return upload;
        });

        when(chunkInfoRepository.findByFileMd5OrderByChunkIndexAsc(anyString())).thenAnswer(invocation -> {
            String fileMd5 = invocation.getArgument(0);
            List<ChunkInfo> result = new ArrayList<>(chunkInfos.getOrDefault(fileMd5, new ArrayList<>()));
            result.sort(Comparator.comparingInt(ChunkInfo::getChunkIndex));
            return result;
        });

        when(chunkInfoRepository.save(any(ChunkInfo.class))).thenAnswer(invocation -> {
            ChunkInfo chunkInfo = invocation.getArgument(0);
            chunkInfos.computeIfAbsent(chunkInfo.getFileMd5(), key -> new ArrayList<>()).add(chunkInfo);
            return chunkInfo;
        });

        ReflectionTestUtils.setField(service, "redisTemplate", redisTemplate);
        ReflectionTestUtils.setField(service, "minioClient", minioClient);
        ReflectionTestUtils.setField(service, "fileUploadRepository", fileUploadRepository);
        ReflectionTestUtils.setField(service, "chunkInfoRepository", chunkInfoRepository);
        ReflectionTestUtils.setField(service, "minioPublicUrl", "http://localhost:9000");
        return service;
    }

    private RedisTemplate<String, Object> buildRedisTemplate() {
        LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory("localhost", 6379);
        connectionFactory.afterPropertiesSet();

        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    private void seedFileUploadRecord() {
        FileUpload fileUpload = new FileUpload();
        fileUpload.setFileMd5(FILE_MD5);
        fileUpload.setFileName(FILE_NAME);
        fileUpload.setTotalSize(ONE_GIB);
        fileUpload.setStatus(0);
        fileUpload.setUserId(USER_ID);
        fileUpload.setOrgTag(ORG_TAG);
        fileUpload.setPublic(false);
        fileUploads.put(fileKey(FILE_MD5, USER_ID), fileUpload);
    }

    private List<Integer> oldGetUploadedChunks(String fileMd5, String userId) {
        List<Integer> uploadedChunks = new ArrayList<>();
        int totalChunks = uploadService.getTotalChunks(fileMd5, userId);
        for (int chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
            Boolean uploaded = redisTemplate.opsForValue().getBit(redisKey(), chunkIndex);
            if (Boolean.TRUE.equals(uploaded)) {
                uploadedChunks.add(chunkIndex);
            }
        }
        return uploadedChunks;
    }

    private long readMergedObjectBytes() throws Exception {
        try (InputStream inputStream = minioClient.getObject(
                GetObjectArgs.builder().bucket(BUCKET).object("merged/" + FILE_MD5).build())) {
            byte[] buffer = new byte[8192];
            long total = 0;
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                total += read;
            }
            return total;
        }
    }

    private void cleanupStorage() throws Exception {
        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder().bucket(BUCKET).prefix("chunks/" + FILE_MD5).recursive(true).build());
        for (Result<Item> result : results) {
            Item item = result.get();
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(BUCKET).object(item.objectName()).build());
        }

        try {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(BUCKET).object("merged/" + FILE_MD5).build());
        } catch (Exception ignored) {
            // 合并文件可能还不存在
        }
    }

    private String redisKey() {
        return "upload:" + USER_ID + ":" + FILE_MD5;
    }

    private String fileKey(String fileMd5, String userId) {
        return userId + "::" + fileMd5;
    }

    private double nsToMs(double ns) {
        return ns / TimeUnit.MILLISECONDS.toNanos(1);
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
