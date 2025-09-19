import os
import json
import secrets
import base64
from datetime import datetime
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

class SecurityManager:
    """セキュリティキー（JWT、VAPID）の自動生成と管理"""

    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        self.security_file = os.path.join(data_dir, 'security_keys.json')
        self.jwt_secret = None
        self.vapid_public_key = None
        self.vapid_private_key = None
        self.vapid_subject = None

    def initialize(self):
        """すべてのセキュリティキーを初期化"""
        # 環境変数をチェック
        env_jwt = os.getenv("JWT_SECRET_KEY")
        env_vapid_public = os.getenv("VAPID_PUBLIC_KEY")
        env_vapid_private = os.getenv("VAPID_PRIVATE_KEY")
        env_vapid_subject = os.getenv("VAPID_SUBJECT", "mailto:admin@hanaview.local")

        # すべてが環境変数にある場合
        if env_jwt and env_vapid_public and env_vapid_private:
            self.jwt_secret = env_jwt
            self.vapid_public_key = env_vapid_public
            self.vapid_private_key = env_vapid_private
            self.vapid_subject = env_vapid_subject
            print("Using security keys from environment variables")
            return True

        # ファイルから読み込み
        if os.path.exists(self.security_file):
            try:
                with open(self.security_file, 'r') as f:
                    keys = json.load(f)
                    self.jwt_secret = keys.get('jwt_secret_key') or env_jwt
                    self.vapid_public_key = keys.get('vapid_public_key') or env_vapid_public
                    self.vapid_private_key = keys.get('vapid_private_key') or env_vapid_private
                    self.vapid_subject = keys.get('vapid_subject', env_vapid_subject)

                    # 不足しているキーがあれば生成
                    modified = False
                    if not self.jwt_secret:
                        self.jwt_secret = self.generate_jwt_secret()
                        modified = True
                        print("Generated new JWT secret key")

                    if not self.vapid_public_key or not self.vapid_private_key:
                        self.generate_vapid_keys()
                        modified = True
                        print("Generated new VAPID keys")

                    if modified:
                        self.save_keys()

                    print("Loaded security keys from file")
                    return True
            except Exception as e:
                print(f"Error loading security keys: {e}")

        # すべて新規生成
        print("Generating new security keys...")
        self.jwt_secret = env_jwt or self.generate_jwt_secret()
        self.vapid_subject = env_vapid_subject

        if not env_vapid_public or not env_vapid_private:
            self.generate_vapid_keys()
        else:
            self.vapid_public_key = env_vapid_public
            self.vapid_private_key = env_vapid_private

        self.save_keys()
        print("New security keys generated and saved")
        return True

    def generate_jwt_secret(self):
        """JWT用の秘密鍵を生成（256ビット）"""
        return secrets.token_hex(32)  # 32 bytes = 256 bits

    def generate_vapid_keys(self):
        """VAPIDキーペアを生成"""
        # ECDSAキーペアを生成
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())

        # Private keyをDER形式でエクスポート
        private_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Public keyを取得
        public_key = private_key.public_key()

        # Public keyをUncompressed Point形式でエクスポート
        public_numbers = public_key.public_numbers()

        # x, y座標を64バイトの形式に変換（各32バイト）
        x_bytes = public_numbers.x.to_bytes(32, byteorder='big')
        y_bytes = public_numbers.y.to_bytes(32, byteorder='big')

        # Uncompressed point format: 0x04 || x || y
        public_raw = b'\x04' + x_bytes + y_bytes

        # Base64 URL-safeエンコード（パディングなし）
        self.vapid_private_key = base64.urlsafe_b64encode(private_der).decode('utf-8').rstrip('=')
        self.vapid_public_key = base64.urlsafe_b64encode(public_raw).decode('utf-8').rstrip('=')

    def save_keys(self):
        """生成したキーをファイルに保存"""
        os.makedirs(self.data_dir, exist_ok=True)

        keys_data = {
            'jwt_secret_key': self.jwt_secret,
            'vapid_public_key': self.vapid_public_key,
            'vapid_private_key': self.vapid_private_key,
            'vapid_subject': self.vapid_subject,
            'created_at': datetime.now().isoformat(),
            'note': 'Auto-generated security keys. DO NOT SHARE!'
        }

        with open(self.security_file, 'w') as f:
            json.dump(keys_data, f, indent=2)

        # ファイルのパーミッションを制限（セキュリティ対策）
        try:
            os.chmod(self.security_file, 0o600)
        except:
            pass  # Windows環境ではchmodが効かない場合があるため

        print(f"Security keys saved to {self.security_file}")
        print("=" * 50)
        print("IMPORTANT: Backup this file for disaster recovery!")
        print(f"File: {self.security_file}")
        print("=" * 50)

# グローバルインスタンス
security_manager = SecurityManager()
