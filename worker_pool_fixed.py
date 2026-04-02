import os
import sys
import time
import json
import uuid
import logging
import requests
import psycopg2
import psycopg2.extras
import hashlib
from datetime import datetime, timedelta
from flask import Flask, jsonify
from threading import Thread

# Add alias for backward compatibility
get_audio_fingerprint = get_cached_fingerprint

# Rest of your worker code here...
