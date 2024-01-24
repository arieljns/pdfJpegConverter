const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const multer = require("multer");
const pdfPoppler = require("pdf-poppler");
const fs = require("fs");
const path = require("path");
const Redis = require("ioredis");
const archiver = require("archiver");

const app = express();

app.use(cors());
app.use(express.json());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
const storage = multer.memoryStorage();

const upload = multer({ storage: storage});

const redis = new Redis();

async function pushToQueue(queueName, data) {
    await redis.rpush(queueName, JSON.stringify(data));
}

async function popFromQueue(queueName) {
    const result = await redis.lpop(queueName);
    return result ? JSON.parse(result) : null;
}

app.post("/upload", upload.array("file"), async (req, res) => {
    const files = req.files;
    const { totalChunks, currentChunk, filename } = req.body;

    try {
        const outputDir = "C:/Users/ariel/Downloads/";
        const zipFilePath = path.join(outputDir, 'converted-images.zip');
        const archive = archiver('zip');
        const output = fs.createWriteStream(zipFilePath);

        archive.pipe(output);

        const conversionPromises = files.map(async (file, i) => {
            const tempFilePath = path.join(outputDir, `temp-${filename}-${i}`);
            fs.appendFileSync(tempFilePath, file.buffer);

            if (currentChunk == totalChunks - 1) {
 
                const options = {
                    format: 'jpeg',
                    out_dir: outputDir,
                    out_prefix: `temp-${filename}-${i}`,
                };

                console.log(`Converting file: ${tempFilePath}`);
                await pdfPoppler.convert(tempFilePath, options);
                console.log(`Conversion successful for file: ${tempFilePath}`);


                const oldFilePath = path.join(outputDir, `temp-${filename}-${i}-0`); // The original file path
                const newFilePath = path.join(outputDir, `converted-${filename}-${i}.jpg`); // The new file path
                fs.renameSync(oldFilePath, newFilePath);

                archive.file(newFilePath, { name: `converted-${filename}-${i}.jpg` });
            }
        });

        await Promise.all(conversionPromises);


        archive.finalize();

        return res.json({
            message: "The file chunks have been received and assembled. Conversion tasks have been enqueued.",
            zipFileUrl: `/download/converted-images.zip`,
        });
    } catch (error) {
        console.error("Error during file processing:", error);
        res.status(500).json({
            message: "Internal Server Error",
            ErrorMessage: error,
        });
    }
});

app.get("/download/converted-images.zip", (req, res) => {
    const zipFilePath = "C:/Users/ariel/Downloads/converted-images.zip";

    console.log(`Checking file existence at path: ${zipFilePath}`);
    if (fs.existsSync(zipFilePath)) {
        console.log("File exists. Proceeding with download.");
        res.download(zipFilePath, "converted-images.zip", (err) => {
            if (err) {
                console.error("Error downloading file:", err);
                res.status(500).json({
                    message: "Internal Server Error",
                    ErrorMessage: err,
                });
            }
        });
    } else {
        console.error("File not found at the specified path.");
        res.status(404).json({
            message: "File not found",
        });
    }
});

const MAX_CONCURRENT_TASKS=5;
async function startWorker() {
    try {
        while (true) {
            const tasks = [];

            for (let i = 0; i < MAX_CONCURRENT_TASKS; i++) {
                const task = await popFromQueue("conversion_queue");
                if (task) {
                    tasks.push(task);
                } else {
                    break; 
                }
            }
            await Promise.all(
                tasks.map(async (task) => {
                    try {
                        const { tempFilePath, options } = task;
                        await pdfPoppler.convert(tempFilePath, options);
                        console.log("Conversion successful for file:", tempFilePath);
                    } catch (error) {
                        console.error("Error during conversion:", error);
                    }
                })
            );

            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    } catch (error) {
        console.error(error);
    }
}

app.listen(8080, async () => {
    console.log("Server is up and running at PORT 8080");
    await startWorker();
});
