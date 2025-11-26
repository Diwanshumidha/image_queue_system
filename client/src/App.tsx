import { useState } from "react";

export default function FileUpload() {
  const [file, setFile] = useState<File | null>(null);
  const [progress, setProgress] = useState<number>(0);
  const [uploading, setUploading] = useState(false);

  const upload = async () => {
    if (!file) return;

    setUploading(true);
    setProgress(0);

    const form = new FormData();
    form.append("file", file);

    const xhr = new XMLHttpRequest(); // Best choice for upload progress

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) {
        const percent = Math.round((event.loaded / event.total) * 100);
        setProgress(percent);
      }
    };

    xhr.onload = async () => {
      setUploading(false);
      if (xhr.status === 200) {
        alert("Uploaded!");
        const response = JSON.parse(xhr.responseText);
        await completeUpload(response.key);
      } else {
        alert("Upload failed: " + xhr.responseText);
      }
    };

    xhr.onerror = () => {
      setUploading(false);
      alert("Network error");
    };

    xhr.open("POST", "http://localhost:8080/upload");
    xhr.send(form);
  };

  const completeUpload = async (key: string) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "http://localhost:8080/upload/complete");
    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.send(JSON.stringify({ key }));
  };

  return (
    <div style={{ width: 300, margin: "40px auto" }}>
      <h2>Upload File</h2>

      <input
        type="file"
        onChange={(e) => setFile(e.target.files?.[0] ?? null)}
      />

      {uploading && (
        <div style={{ marginTop: "10px" }}>
          <div
            style={{
              width: "100%",
              height: "10px",
              background: "#ddd",
              borderRadius: 4,
            }}
          >
            <div
              style={{
                width: `${progress}%`,
                height: "100%",
                background: "#4caf50",
                borderRadius: 4,
                transition: "width 0.1s",
              }}
            ></div>
          </div>
          <p>{progress}%</p>
        </div>
      )}

      <button
        onClick={upload}
        disabled={!file || uploading}
        style={{
          marginTop: 15,
          padding: "8px 16px",
          cursor: uploading ? "not-allowed" : "pointer",
        }}
      >
        {uploading ? "Uploading..." : "Upload"}
      </button>
    </div>
  );
}
