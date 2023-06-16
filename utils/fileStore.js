import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import {
  calculateFileHash,
  getFilesInDirectory,
  deleteFromLocal,
} from "./fileHelper.js";
import dotenv from "dotenv";
dotenv.config();

// File store bucket name
const fileStoreBucketName =
  process.env.AWS_FILE_STORE_BUCKET_NAME || "sherlock-inmemorysearch";

// Create S3 client
const fileStore = new S3Client({
  endpoint: `https://${process.env.AWS_ACCOUNT_ID_FILE_STORE}.r2.cloudflarestorage.com`,
  region: `${process.env.region}`,
  credentials: {
    accessKeyId: `${process.env.AWS_ACCOUNT_ACCESS_KEY_ID_FILE_STORE}`,
    secretAccessKey: `${process.env.AWS_ACCOUNT_ACCESS_KEY_SECRET_FILE_STORE}`,
  },
});

// Function to upload a file to S3
const uploadToS3 = async (file, s3FilePath) => {
  const fileContent = await fs.readFile(file);
  const uploadParams = {
    Bucket: fileStoreBucketName,
    Key: s3FilePath,
    Body: fileContent,
  };
  await fileStore.send(new PutObjectCommand(uploadParams));
};

// Function to delete a file from S3
const deleteFromS3 = async (s3FilePath) => {
  const deleteParams = {
    Bucket: fileStoreBucketName,
    Key: s3FilePath,
  };
  await fileStore.send(new DeleteObjectCommand(deleteParams));
};

// Function to get files from S3
const getFilesInS3Bucket = async (s3Path, fileList = []) => {
  const listParams = {
    Bucket: fileStoreBucketName,
    Prefix: s3Path,
  };
  const s3FilesResponse = await fileStore.send(
    new ListObjectsV2Command(listParams)
  );
  for (const file of s3FilesResponse.Contents) {
    const filePath = file.Key;
    if (filePath.endsWith("/")) {
      fileList = await getFilesInS3Bucket(filePath, fileList);
    } else {
      fileList.push(filePath);
    }
  }
  return fileList;
};

// Function to download a file from S3
const downloadFromS3 = async (s3FilePath, localFilePath) => {
  const downloadParams = {
    Bucket: fileStoreBucketName,
    Key: s3FilePath,
  };
  const { Body } = await fileStore.send(new GetObjectCommand(downloadParams));
  await fs.writeFile(localFilePath, Body);
};

// Compare local files to S3 and sync
export const saveDataToS3 = async (localPath, s3Path) => {
  if (!fileStore) {
    console.log("[saveDataToS3] s3 is not defined");
    return;
  }

  console.log(
    `\n.\n.\n.\n\n------------\n1. BEGINNING SYNC FROM ${localPath} TO ${s3Path}\n------------`
  );

  console.log(`\nGETTING S3 STATUS:`);
  const listObjectsParams = {
    Bucket: fileStoreBucketName,
    Prefix: s3Path,
  };
  const s3FilesResponse = await fileStore.send(
    new listObjectsV2Command(listObjectsParams)
  );
  const s3Files = s3FilesResponse.Contents.map((file) => file.Key);
  console.log("\nThese are the s3Files to compare against: ", s3Files);

  console.log(
    `\n----------\n2-x. SYNCING FROM LOCAL TO S3 NOW STARTING\n----------`
  );

  const localFiles = await getFilesInDirectory(localPath);
  for (const file of localFiles) {
    console.log(`\n-------\nSYNCING LOCAL FILE: \n${file}`);

    const s3FilePath = path.join(s3Path, path.relative(localPath, file));

    console.log(
      "\nThis is the s3FilePath (AKA, the Key/Name) to find/create: ",
      s3FilePath
    );

    console.log(`\nBEGINNING FILE CHECK #1: Does ${file} already exist in S3?`);

    if (!s3Files.includes(s3FilePath)) {
      console.log(`\nWARNING: s3Files does not include ${s3FilePath}`);
      await uploadToS3(file, s3FilePath);
      console.log(`RESOLVED: ${s3FilePath} has now been uploaded`);
    } else {
      console.log(`\nPASSED FILE CHECK #1: s3Files includes ${s3FilePath}`);
      console.log(
        `\nCONTINUING TO FILE CHECK #1.5: Does S3File match localFile?`
      );
      const getObjectParams = {
        Bucket: fileStoreBucketName,
        Key: s3FilePath,
      };
      const s3FileResponse = await fileStore.send(
        new getObjectCommand(getObjectParams)
      );

      // Get the file content's hash values
      const s3FileHash = crypto
        .createHash("sha256")
        .update(s3FileResponse.Body)
        .digest("hex");
      const localFileHash = await calculateFileHash(file);

      // Compare the hash values & sync if mismatch
      if (s3FileHash !== localFileHash) {
        console.log(
          `\nWARNING: The body of ${s3FilePath} does not match the body of ${file}`
        );
        await uploadToS3(file, s3FilePath);
        console.log(
          `RESOLVED: ${s3FilePath} has been updated with the local data`
        );
      } else {
        console.log(`\nPASSED FILE CHECK #1.5: ${s3FilePath} matches ${file}`);
      }
    }
    console.log(`\nSYNCING LOCAL TO S3 DONE`);
  }

  console.log(
    `\n-------\n3. DELETING FILES FROM S3 NO LONGER IN LOCAL\n-------`
  );
  for (const file of s3Files) {
    const localFilePath = path.join(localPath, path.relative(s3Path, file));
    console.log(`\nFILE CHECK #3: Does ${file} still exist in local?`);
    if (!localFiles.includes(localFilePath)) {
      console.log(`localPath to search for: ${localFilePath}`);
      console.log(`\nWARNING: ${file} not found in local and must be deleted`);
      await deleteFromS3(file);
      console.log(`RESOLVED: ${file} has been deleted from S3`);
    }
    console.log(
      `PASSED FILE CHECK #2: ${file} exists in local and does not need to be deleted`
    );
  }
  console.log(`\n-----------\n4. SYNCING DONE\n-----------`);
};

// Function to load files from S3 to local/server
export const loadDataFromS3 = async (s3Path, localPath) => {
  if (!fileStore) {
    console.log("[loadDataFromS3] s3 is not defined");
    return;
  }
  console.log(
    `\n.\n.\n.\n\n------------\n1. BEGINNING SYNC FROM ${s3Path} TO ${localPath}\n------------`
  );
  const localFiles = await getFilesInDirectory(localPath);
  console.log(`\nGETTING S3 STATUS:`);
  const s3Files = await getFilesInS3Bucket(s3Path);
  console.log("\nThese are the s3Files to compare against: ", s3Files);

  console.log(
    `\n----------\n2-x. SYNCING FROM S3 TO LOCAL NOW STARTING\n----------`
  );
  for (const file of s3Files) {
    console.log(`\n-------\nSYNCING S3 FILE: \n${file}`);
    const localFilePath = path.join(localPath, path.relative(s3Path, file));
    console.log("\nThis is the localFilePath to find/create: ", localFilePath);
    console.log(
      `\nBEGINNING FILE CHECK #1: Does ${file} already exist in local?`
    );
    if (!localFiles.includes(localFilePath)) {
      console.log(`\nWARNING: localFiles does not include ${localFilePath}`);
      await downloadFromS3(file, localFilePath);
      console.log(`RESOLVED: ${localFilePath} has now been downloaded`);
    } else {
      console.log(
        `\nPASSED FILE CHECK #1: localFiles includes ${localFilePath}`
      );
      console.log(
        `\nCONTINUING TO FILE CHECK #1.5: Does localFile match S3File?`
      );

      const getObjectParams = {
        Bucket: fileStoreBucketName,
        Key: file,
      };
      const s3FileResponse = await fileStore.send(
        new getObjectCommand(getObjectParams)
      );
      const localFileHash = await calculateFileHash(localFilePath);
      const s3FileHash = crypto
        .createHash("sha256")
        .update(s3FileResponse.Body)
        .digest("hex");

      if (localFileHash !== s3FileHash) {
        console.log(
          `\nWARNING: The body of ${localFilePath} does not match the body of ${file}`
        );
        await downloadFromS3(file, localFilePath);
        console.log(
          `RESOLVED: ${localFilePath} has been updated with the S3 data`
        );
      } else {
        console.log(
          `\nPASSED FILE CHECK #1.5: ${localFilePath} matches ${file}`
        );
      }
    }
    console.log(`\nSYNCING S3 TO LOCAL DONE`);
  }

  console.log(
    `\n-------\n3. DELETING FILES FROM LOCAL NO LONGER IN S3\n-------`
  );
  for (const file of localFiles) {
    const s3FilePath = path.join(s3Path, path.relative(localPath, file));
    console.log(`\nFILE CHECK #2: Does ${file} still exist in S3?`);
    if (!s3Files.includes(s3FilePath)) {
      console.log(`s3Path to search for: ${s3FilePath}`);
      console.log(`\nWARNING: ${file} not found in S3 and must be deleted`);
      await deleteFromLocal(file);
      console.log(`RESOLVED: ${file} has been deleted from local`);
    }
    console.log(
      `PASSED FILE CHECK #2: ${file} exists in S3 and does not need to be deleted`
    );
  }
  console.log(`\n-----------\n4. SYNCING DONE\n-----------`);
};
