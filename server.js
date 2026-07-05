// server.js
import express from "express";
import http from "http";
import { Server } from "socket.io";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import pngToIcoRoute from "./image/pngToIco.js";
import {
  S3Client,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import multer from "multer";
import { Upload } from "@aws-sdk/lib-storage";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
import cors from "cors";

app.use(cors({
  origin: "http://localhost:3000",
  methods: ["GET", "POST"],
  credentials: false
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use("/api", pngToIcoRoute);

// === SOCKET.IO SETUP ===
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 100 * 1024 * 1024, // 10MB per message
  pingTimeout: 60000,
  pingInterval: 25000,
});

app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true, limit: "50mb" }));
server.timeout = 0; // no request timeout

// === S3 CLIENT ===
const s3Client = new S3Client({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
  region: process.env.AWS_REGION,
});

const S3_BUCKET = process.env.AWS_BUCKET_NAME;
if (!S3_BUCKET) {
  console.error("❌ AWS_BUCKET_NAME not set in .env");
  process.exit(1);
}

// Active multipart uploads
const activeMultipartUploads = new Map(); // { uploadId: { Key, roomId, timeoutHandle } }
const roomMessages = {}; // last 5 messages per room
const activeFiles = new Map();
// s3Key ->
// {
//   roomId,
//   ownerId,
//   timeoutHandle,
//   expiresAt,
//   updated:false
// }

// === Helper: auto-abort multipart uploads ===
function scheduleMultipartAbort(uploadId, key, ttlMs = 300 * 60 * 1000, roomId = null) {
  if (activeMultipartUploads.has(uploadId)) {
    clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
  }
  const timeoutHandle = setTimeout(async () => {
    try {
      await s3Client.send(
        new AbortMultipartUploadCommand({ Bucket: S3_BUCKET, Key: key, UploadId: uploadId })
      );
      activeMultipartUploads.delete(uploadId);
      if (roomId) io.to(roomId).emit("file-removed", { s3Key: key });
      console.log(`🛑 Aborted multipart ${uploadId} and cleaned up ${key}`);
    } catch (err) {
      console.error("Error aborting multipart:", err);
    }
  }, ttlMs);

  activeMultipartUploads.set(uploadId, { Key: key, timeoutHandle, roomId });
}

// === SOCKET.IO HANDLERS ===
io.on("connection", (socket) => {
  console.log("✅ Client connected:", socket.id);

  // Join room
  socket.on("join-room", (roomId) => {

    socket.join(roomId);

    if (!roomMessages[roomId]) {
      roomMessages[roomId] = {
        messages: [],
        contentType: "text",
        currentText: ""
      };
    }

    const messages = roomMessages[roomId].messages.map(msg => {

    const file = activeFiles.get(msg.s3Key);

    if (file) {

        return {
            ...msg,
            expiresAt: file.expiresAt,
            timerStarted: file.timerStarted
        };

    }

    return msg;

});

socket.emit("room-messages", messages);

    socket.emit(
      "room-contentType",
      roomMessages[roomId].contentType
    );

    socket.emit(
      "room-current-text",
      roomMessages[roomId].currentText || ""
    );
  });

  // Handle content type changes
  socket.on("room-contentType", ({ roomId, type }) => {
    if (!roomMessages[roomId]) roomMessages[roomId] = { messages: [], contentType: "text" };
    roomMessages[roomId].contentType = type;

    // Broadcast to all users in the room
    io.to(roomId).emit("room-contentType", type);
  });

  // Handle text messages
  socket.on("room-message", ({
    roomId,
    type,
    content,
    fileName,
    fileType,
    data,
    uploadedAt,
    s3Key
  }) => {

    if (!roomMessages[roomId]) {
      roomMessages[roomId] = {
        messages: [],
        contentType: "text",
        currentText: ""
      };
    }

    if (type === "text") {

      roomMessages[roomId].currentText = content;

      io.to(roomId).emit("room-message", {
        type: "text",
        content
      });

      return;
    }

    const message = {
      type,
      fileName,
      fileType,
      data,
      uploadedAt,
      s3Key,
      removed: false
    };

    roomMessages[roomId].messages.push(message);

    if (roomMessages[roomId].messages.length > 5) {
      roomMessages[roomId].messages.shift();
    }

    io.to(roomId).emit("room-message", message);
  });

  // Step 1: Initiate multipart upload
  // Step 1: Initiate multipart upload
  socket.on("initiate-multipart", async ({ roomId, fileName, fileType }) => {
    try {
      const Key = `${Date.now()}_${fileName}`;

      const createCmd = new CreateMultipartUploadCommand({
        Bucket: S3_BUCKET,
        Key,
        ContentType: fileType,
      });

      const { UploadId } = await s3Client.send(createCmd);

      // ✅ Store on socket session
      if (!socket.uploadSessions) {
        socket.uploadSessions = {};
      }
      socket.uploadSessions[UploadId] = { Key };

      // ✅ Also track globally so 
      // tipart can see it
      activeMultipartUploads.set(UploadId, {
        Key,
        roomId,
        timeoutHandle: null,
      });

      // auto-abort if no activity after 4 mins
      scheduleMultipartAbort(UploadId, Key, 30 * 60 * 1000, roomId);

      socket.emit("multipart-initiated", { uploadId: UploadId, key: Key });
      console.log(`✅ Multipart upload started: ${Key}, UploadId: ${UploadId}`);
    } catch (err) {
      console.error("initiate-multipart error:", err);
      socket.emit("initiate-error", { message: err.message });
    }
  });


  // Step 2: Get presigned URLs for part numbers
  // Step 2: Provide presigned URLs for given parts
  socket.on("get-presigned-urls", async ({ uploadId, partNumbers }) => {
    try {
      if (!socket.uploadSessions || !socket.uploadSessions[uploadId]) {
        socket.emit("presign-error", { message: "UploadId not found or expired" });
        return;
      }

      const { Key } = socket.uploadSessions[uploadId];
      const urls = [];

      for (const partNumber of partNumbers) {
        const cmd = new UploadPartCommand({
          Bucket: S3_BUCKET,
          Key,
          UploadId: uploadId,
          PartNumber: partNumber,
        });

        const url = await getSignedUrl(s3Client, cmd, { expiresIn: 3600 });
        urls.push({ partNumber, url });
      }

      socket.emit("presigned-urls", { uploadId, urls });
      console.log(`✅ Sent presigned URLs for ${Key} (parts: ${partNumbers.length})`);
    } catch (err) {
      console.error("get-presigned-urls error:", err);
      socket.emit("presign-error", { message: err.message });
    }
  });


  // Step 3: Complete multipart upload
  socket.on("complete-multipart", async ({ uploadId, parts }) => {
    try {
      if (!activeMultipartUploads.has(uploadId)) {
        socket.emit("complete-error", { message: "UploadId not found or expired" });
        return;
      }
      const { Key, roomId } = activeMultipartUploads.get(uploadId);
      console.log("UPLOAD ID:", uploadId);
      console.log("KEY:", Key);
      console.log("PARTS:");
      console.log(JSON.stringify(parts, null, 2));
      const res = await s3Client.send(
        new CompleteMultipartUploadCommand({
          Bucket: S3_BUCKET,
          Key,
          UploadId: uploadId,
          MultipartUpload: { Parts: parts },
        })
      );

      clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
      activeMultipartUploads.delete(uploadId);

      const location =
        `https://codesharuiii.onrender.com/download/${encodeURIComponent(Key)}`;
      const fileMessage = {
        type: "file",
        fileName: path.basename(Key),
        data: location,
        s3Key: Key,
        uploadedAt: Date.now(),

        removed: false,
        timerStarted: false,
        expiresAt: null
      };

      if (!roomMessages[roomId]) roomMessages[roomId] = { messages: [], contentType: "text" };
      roomMessages[roomId].messages.push(fileMessage);
      if (roomMessages[roomId].messages.length > 5) roomMessages[roomId].messages.shift();

      io.to(roomId).emit("room-message", fileMessage);
      socket.emit("complete-success", {
        uploadId,
        location,
        s3Key: Key
      });

      // Delete after 4 min
      const timeoutHandle = setTimeout(() => {

        const file = activeFiles.get(Key);

        if (!file) return;

        if (file.timerStarted) return;

        file.timerStarted = true;

        file.expiresAt = Date.now() + 20 * 60 * 1000;
        const msg = roomMessages[file.roomId].messages.find(
    m => m.s3Key === Key
);

if (msg) {
    msg.timerStarted = true;
    msg.expiresAt = file.expiresAt;
}

        io.to(roomId).emit("timer-started", {
          s3Key: Key,
          expiresAt: file.expiresAt
        });

        file.timeoutHandle = setTimeout(async () => {

          try {

            await s3Client.send(
              new DeleteObjectCommand({
                Bucket: S3_BUCKET,
                Key
              })
            );
            const msg = roomMessages[file.roomId].messages.find(
              m => m.s3Key === s3Key
            );

            if (msg) {
              msg.removed = true;
            }

            io.to(roomId).emit("file-removed", {
              s3Key: Key
            });

            activeFiles.delete(Key);

          } catch (err) {

            console.error(err);

          }

        }, 20 * 60 * 1000);

      }, 20 * 60 * 1000);

      activeFiles.set(Key, {
        roomId,
        timeoutHandle,
        uploadedAt: Date.now(),
        expiresAt: null,
        timerStarted: false,
        updated: false
      });

      console.log(`✅ Completed upload: ${location}`);
    } catch (err) {
      console.error("complete-multipart error:", err);
      socket.emit("complete-error", { message: err.message });
    }
  });

  socket.on("delete-file", async ({ s3Key }) => {

    const file = activeFiles.get(s3Key);
    if (!file) return;

    clearTimeout(file.timeoutHandle);

    await s3Client.send(
      new DeleteObjectCommand({
        Bucket: S3_BUCKET,
        Key: s3Key
      })
    );

    const msg = roomMessages[file.roomId].messages.find(
      m => m.s3Key === s3Key
    );

    if (msg) {
      msg.removed = true;
    }

    io.to(file.roomId).emit("file-removed", {
      s3Key
    });

    activeFiles.delete(s3Key);
    socket.emit("file-delete-success", {
      message: "File deleted successfully."
    });

  });
  socket.on("change-expiry", ({ s3Key, minutes }) => {

    const file = activeFiles.get(s3Key);
    console.log("the current file", file);

    if (!file) return;

    clearTimeout(file.timeoutHandle);

    file.timeoutHandle = setTimeout(async () => {

      await s3Client.send(
        new DeleteObjectCommand({
          Bucket: S3_BUCKET,
          Key: s3Key
        })
      );
      const msg = roomMessages[file.roomId].messages.find(
        m => m.s3Key === s3Key
      );

      if (msg) {
        msg.removed = true;
      }

      io.to(file.roomId).emit("file-removed", {
        s3Key
      });
      console.log("file deleted successfully");

      activeFiles.delete(s3Key);

    }, minutes * 60 * 1000);

    file.timerStarted = true;

    file.expiresAt = Date.now() + minutes * 60 * 1000;
    const msg = roomMessages[file.roomId].messages.find(
      m => m.s3Key === s3Key
    );

    if (msg) {
      msg.timerStarted = true;
      msg.expiresAt = file.expiresAt;
    }

    io.to(file.roomId).emit("timer-started", {
      s3Key,
      expiresAt: file.expiresAt
    });
    socket.emit("timer-updated-success", {
      message: `Timer updated successfully to ${minutes} minute(s).`
    });

  });

  socket.on("disconnect", () => {
    console.log("❌ Client disconnected:", socket.id);
  });
});

// === REST FALLBACK ENDPOINTS (optional) ===

// Presign many parts
app.post("/presign-multipart", async (req, res) => {
  try {
    const { fileName, fileType, partsCount = 1 } = req.body;
    if (!fileName) return res.status(400).json({ message: "fileName required" });

    const key = `uploads/${Date.now()}-${fileName}`;
    const createRes = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: S3_BUCKET, Key: key, ContentType: fileType })
    );

    const uploadId = createRes.UploadId;
    scheduleMultipartAbort(uploadId, key);

    const urls = [];
    for (let i = 1; i <= partsCount; i++) {
      const cmd = new UploadPartCommand({ Bucket: S3_BUCKET, Key: key, UploadId: uploadId, PartNumber: i });
      const url = await getSignedUrl(s3Client, cmd, { expiresIn: 3600 });
      urls.push({ partNumber: i, url });
    }

    res.json({ uploadId, key, urls });
  } catch (err) {
    console.error("presign-multipart error:", err);
    res.status(500).json({ message: err.message });
  }
});

// Complete multipart
app.post("/complete-multipart", async (req, res) => {
  try {
    const { uploadId, key, parts } = req.body;
    if (!uploadId || !key || !parts) return res.status(400).json({ message: "uploadId, key and parts required" });

    const result = await s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: S3_BUCKET,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: { Parts: parts },
      })
    );

    if (activeMultipartUploads.has(uploadId)) {
      clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
      activeMultipartUploads.delete(uploadId);
    }

    res.json({ message: "Completed", location: result.Location || `s3://${S3_BUCKET}/${key}` });
  } catch (err) {
    console.error("complete-multipart REST error:", err);
    res.status(500).json({ message: err.message });
  }
});

// Abort multipart
app.post("/abort-multipart", async (req, res) => {
  try {
    const { uploadId, key } = req.body;
    if (!uploadId || !key) return res.status(400).json({ message: "uploadId and key required" });

    await s3Client.send(new AbortMultipartUploadCommand({ Bucket: S3_BUCKET, Key: key, UploadId: uploadId }));
    if (activeMultipartUploads.has(uploadId)) {
      clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
      activeMultipartUploads.delete(uploadId);
    }
    res.json({ message: "Aborted" });
  } catch (err) {
    console.error("abort-multipart error:", err);
    res.status(500).json({ message: err.message });
  }
});

// Single small file upload (fallback)
const upload = multer({ storage: multer.memoryStorage() });
app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    const key = `uploads/${Date.now()}-${file.originalname}`;

    const uploader = new Upload({
      client: s3Client,
      params: { Bucket: S3_BUCKET, Key: key, Body: file.buffer, ContentType: file.mimetype },
    });

    const result = await uploader.done();
    res.json({ fileName: file.originalname, s3Url: result.Location || `s3://${S3_BUCKET}/${key}` });
  } catch (err) {
    console.error("upload error:", err);
    res.status(500).json({ message: err.message });
  }
});
app.get("/download/:key", async (req, res) => {
  try {
    const key = decodeURIComponent(req.params.key);

    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: S3_BUCKET,
        Key: key,
      })
    );

    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${path.basename(key)}"`
    );

    res.setHeader(
      "Content-Type",
      "application/octet-stream"
    );

    response.Body.pipe(res);

  } catch (err) {
    console.error(err);

    if (
      err.name === "NoSuchKey" ||
      err.Code === "NoSuchKey" ||
      err.$metadata?.httpStatusCode === 404
    ) {
      return res.status(404).json({
        message: "File removed from server"
      });
    }

    return res.status(500).json({
      message: "Download failed"
    });
  }
});
setInterval(() => {

  activeFiles.forEach((file, key) => {

    if (file.updated) return;


    if (!file.timerStarted) {

      const passed =
        Date.now() - file.uploadedAt;

      if (passed >= 20 * 60 * 1000) {

        clearTimeout(file.timeoutHandle);

        file.timeoutHandle = setTimeout(async () => {

          await s3Client.send(
            new DeleteObjectCommand({
              Bucket: S3_BUCKET,
              Key: key
            })
          );

          io.to(file.roomId).emit("file-removed", {
            s3Key: key
          });

          activeFiles.delete(key);

        }, 20 * 60 * 1000);

        file.updated = true;
        file.timerStarted = true;
        file.expiresAt = Date.now() + 20 * 60 * 1000;

        const msg = roomMessages[file.roomId].messages.find(
          m => m.s3Key === key
        );

        if (msg) {
          msg.timerStarted = true;
          msg.expiresAt = file.expiresAt;
        }

        io.to(file.roomId).emit("timer-started", {
          s3Key: key,
          expiresAt: file.expiresAt
        });
      }
    }
  });

}, 60000);

// === START SERVER ===
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => console.log(`🚀 Server running at http://localhost:${PORT}`));
