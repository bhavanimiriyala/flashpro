package com.mss.gcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

@RestController
@Slf4j
public class GcpStorageController {
	@Getter(AccessLevel.PROTECTED)
	@Setter(AccessLevel.PROTECTED)
	@Autowired
	private Storage storage;

	@Value("bucketname")
	String bucketName;
	@Value("subdirectory")
	String subdirectory;

	@PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public Mono<URL> uploadFile(@RequestPart("file") FilePart filePart) {

		final byte[] byteArray = convertToByteArray(filePart);

		final BlobId blobId = constructBlobId(bucketName, subdirectory, filePart.filename());
		return Mono.just(blobId).map(bId -> BlobInfo.newBuilder(blobId).setContentType("text/plain").build())
				.doOnNext(blobInfo -> getStorage().create(blobInfo, byteArray))
				.map(blobInfo -> createSignedPathStyleUrl(blobInfo, 10, TimeUnit.MINUTES));
	}

	private URL createSignedPathStyleUrl(BlobInfo blobInfo, int duration, TimeUnit timeUnit) {
		return getStorage().signUrl(blobInfo, duration, timeUnit, Storage.SignUrlOption.withPathStyle());
	}

	private BlobId constructBlobId(String bucketName, @Nullable String subdirectory, String fileName) {
		return Optional.ofNullable(subdirectory).map(s -> BlobId.of(bucketName, subdirectory + "/" + fileName))
				.orElse(BlobId.of(bucketName, fileName));
	}

	@SneakyThrows
	private byte[] convertToByteArray(FilePart filePart) {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			filePart.content().subscribe(dataBuffer -> {
				byte[] bytes = new byte[dataBuffer.readableByteCount()];
				log.trace("readable byte count:" + dataBuffer.readableByteCount());
				dataBuffer.read(bytes);
				DataBufferUtils.release(dataBuffer);
				try {
					bos.write(bytes);
				} catch (IOException e) {
					log.error("read request body error...", e);
				}
			});
			return bos.toByteArray();
		}
	}
}
