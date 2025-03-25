import boto3
import subprocess
import os
import tempfile
import json
from datetime import datetime

# AWS S3 configuration
s3_client = boto3.client('s3')

# Metadata file to track processed videos (stored in S3)
METADATA_KEY = "processed_videos.json"
METADATA_LOCAL = "processed_videos.json"

def load_processed_videos(bucket_name):
    """Load the list of processed videos from S3 or initialize if not exists."""
    processed = {}
    try:
        s3_client.download_file(bucket_name, METADATA_KEY, METADATA_LOCAL)
        with open(METADATA_LOCAL, 'r') as f:
            processed = json.load(f)
    except s3_client.exceptions.NoSuchKey:
        print("No metadata file found, starting fresh.")
    return processed

def save_processed_videos(bucket_name, processed):
    """Save the updated list of processed videos to S3."""
    with open(METADATA_LOCAL, 'w') as f:
        json.dump(processed, f, indent=2)
    s3_client.upload_file(METADATA_LOCAL, bucket_name, METADATA_KEY)
    print(f"Updated processed videos metadata in s3://{bucket_name}/{METADATA_KEY}")

def download_video(bucket_name, video_key, local_path):
    """Download video from S3."""
    print(f"Downloading {video_key}...")
    s3_client.download_file(bucket_name, video_key, local_path)
    return local_path

def extract_stills(video_path, output_dir, frame_rate=1):
    """Extract stills from video using FFmpeg."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    output_pattern = os.path.join(output_dir, "frame_%04d.jpg")
    cmd = [
        "ffmpeg",
        "-i", video_path,
        "-vf", f"fps={frame_rate}",
        "-q:v", "2",
        output_pattern,
        "-y"
    ]
    
    print(f"Extracting stills to {output_dir}...")
    subprocess.run(cmd, check=True)

def upload_stills_to_s3(bucket_name, output_dir, video_key, s3_prefix="stills/"):
    """Upload stills to S3 under a folder named after the video."""
    stills_folder = f"{s3_prefix}{os.path.splitext(os.path.basename(video_key))[0]}/"
    for filename in os.listdir(output_dir):
        if filename.endswith(".jpg"):
            local_path = os.path.join(output_dir, filename)
            s3_key = f"{stills_folder}{filename}"
            print(f"Uploading {filename} to {bucket_name}/{s3_key}...")
            s3_client.upload_file(local_path, bucket_name, s3_key)

def get_new_videos(bucket_name, prefix="", processed_videos=None):
    """List videos in S3 that haven't been processed yet."""
    if processed_videos is None:
        processed_videos = {}
    
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    all_videos = {obj['Key']: obj['LastModified'] for obj in response.get('Contents', []) 
                  if obj['Key'].endswith(('.mp4', '.mov', '.avi'))}  # Add more video extensions as needed
    
    new_videos = {key: last_modified for key, last_modified in all_videos.items() 
                  if key not in processed_videos}
    return new_videos

def main():
    # Configuration
    bucket_name = "2samples-static-assets-211125453069"
    video_prefix = "videos/"  # S3 prefix where videos are stored
    frame_rate = 1

    # Load previously processed videos
    processed_videos = load_processed_videos(bucket_name)

    # Find new videos
    new_videos = get_new_videos(bucket_name, video_prefix, processed_videos)
    if not new_videos:
        print("No new videos found to process.")
        return

    print(f"Found {len(new_videos)} new video(s) to process.")

    # Process each new video
    with tempfile.TemporaryDirectory() as temp_dir:
        for video_key, last_modified in new_videos.items():
            video_path = os.path.join(temp_dir, "video_temp")
            output_dir = os.path.join(temp_dir, "stills")

            # Download, process, and upload
            download_video(bucket_name, video_key, video_path)
            extract_stills(video_path, output_dir, frame_rate)
            upload_stills_to_s3(bucket_name, output_dir, video_key)

            # Mark as processed
            processed_videos[video_key] = last_modified.isoformat()

    # Save updated metadata
    save_processed_videos(bucket_name, processed_videos)
    print("Processing complete!")

if __name__ == "__main__":
    main()
