# S3 Video to Stills
A Python tool to extract stills from a video in an AWS S3 bucket.

## Prerequisites
- Python 3.x
- FFmpeg installed (`brew install ffmpeg` on macOS, `sudo apt install ffmpeg` on Ubuntu)
- AWS CLI configured with credentials

## Setup
1. Clone the repo: `git clone <repo-url>`
2. Install dependencies: `pip install -r requirements.txt`
3. Update `video_to_stills.py` with your S3 bucket and video key.

## Usage
Run the script: `python video_to_stills.py`
