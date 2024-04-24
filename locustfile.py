"""
Run using 'locust -f locustfile.py'
default ip&port : http://localhost:8089/
"""

from locust import HttpUser,task,between


access_token = "eyJ0eXAiOiJKV1QiLCJub25jZSI6IlotWVVyNW5SejNBQ0x2X2stdDF0SGY0ckJEaXN2cV9iZTgxcW1BaDA5ZFkiLCJhbGciOiJSUzI1NiIsIng1dCI6InEtMjNmYWxldlpoaEQzaG05Q1Fia1A1TVF5VSIsImtpZCI6InEtMjNmYWxldlpoaEQzaG05Q1Fia1A1TVF5VSJ9.eyJhdWQiOiIwMDAwMDAwMy0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDAiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC9mYWIzOTQ5ZC0zY2IwLTQwOTctODAyZi0wNTNhZDcwNzUyMTkvIiwiaWF0IjoxNzEzOTM0Mjc3LCJuYmYiOjE3MTM5MzQyNzcsImV4cCI6MTcxMzkzOTM2MiwiYWNjdCI6MCwiYWNyIjoiMSIsImFpbyI6IkFUUUF5LzhXQUFBQVlEU3hPNmZBL2xod2M0L0ZUTVRxdW5QVEJzNDhQV20rd1JLWFBYMFEvcjBEZXNUUmtyQjJFRzBHZFhRbmZlVzgiLCJhbXIiOlsicHdkIl0sImFwcF9kaXNwbGF5bmFtZSI6IkN1c3RvbWVyMzYwIC0gREVWIiwiYXBwaWQiOiIzYjEwZTkzYS0zNTRlLTRlOWUtODUyYS0wMWY0NTc5OTg2ZjMiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6IlMiLCJnaXZlbl9uYW1lIjoiUHJhZGlzaCIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjI0MDY6NzQwMDpmZjAzOmYyNDI6ZmM5YzplODg6ZTgyODo0MTRhIiwibmFtZSI6IlByYWRpc2ggUyIsIm9pZCI6IjRiM2UyNTM2LWY5ZjYtNGJmNS05NmNhLTljYTNiZjVmNjJiNCIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0zNzgzNTQxNjYyLTMxNjUzODkxNzUtMzk0MDk4NDQ2LTExNDE1MCIsInBsYXRmIjoiMyIsInB1aWQiOiIxMDAzMjAwMkI2MzIwRkNGIiwicmgiOiIwLkFUMEFuWlN6LXJBOGwwQ0FMd1U2MXdkU0dRTUFBQUFBQUFBQXdBQUFBQUFBQUFBOUFBQS4iLCJzY3AiOiJEaXJlY3RvcnkuUmVhZC5BbGwgb3BlbmlkIHByb2ZpbGUgVXNlci5SZWFkIGVtYWlsIiwic2lnbmluX3N0YXRlIjpbImttc2kiXSwic3ViIjoidm5wR2lBYjE3SXZ4N01EZXI2UEMzdmp3bzNpeGNkN3Y0ZXFleE5oYjhyUSIsInRlbmFudF9yZWdpb25fc2NvcGUiOiJBUyIsInRpZCI6ImZhYjM5NDlkLTNjYjAtNDA5Ny04MDJmLTA1M2FkNzA3NTIxOSIsInVuaXF1ZV9uYW1lIjoiUHJhZGlzaC5TQGNlYnVwYWNpZmljYWlyLmNvbSIsInVwbiI6IlByYWRpc2guU0BjZWJ1cGFjaWZpY2Fpci5jb20iLCJ1dGkiOiJyMFNXSEtMWVoweUlMT04zbE5UQUFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX3N0Ijp7InN1YiI6Ikd3VUpQc2RLc1VBbE91TVZKNVdRR2V0dmR3X1RVMzdlTDVaSnVPcDhRVXMifSwieG1zX3RjZHQiOjE1MDgxMjI2MTd9.HGkjpnwf_pTrMCos751SuonqrYLWh6OwCnQu1_y2wRk3XjaM2yCxgUrUaAj_K3W_g9TF796q4iX-NeTwmcit5e-H2BPwNShECLcocBY42aABnG-T6i9uuTuUe8zkjVQmBtSezeSisUKzpKRXib03eE9jqFxZZlaiux50RjVjsIvect4kE2PoqMLp-ppuTxDpL0m6vni5UPFVGSmp90Xunob2e5FePXSGzR6Q2YucMuWS0ovTDLVUznJrsSu9MoSVHcVMKjFrCoQDJRDfv4wKJIxhtwBq5dWuLCPzHmZR6zGUTvmhWNtowDkoseLjlQ1OtndIJ7T-YLLE1wXal_0ilA"
class AppUser(HttpUser):
    wait_time = between(2,5)

    @task
    def milestones(self):

        headers = {
            "Access-token": access_token,
        }
        self.client.get("/api/milestones",headers=headers)

    @task
    def profile(self):
        headers = {
            "Access-token": access_token,
        }
        self.client.get("/api/profile?profile_type=passenger&id=lPxgQpmbWM0qdjVaLuYAL4ctbqQ",headers=headers)

    @task
    def profile_search(self):
        headers = {
            "Access-token": access_token,
        }
        self.client.get("/api/profile/search?profile_type=passenger&firstname=Ryan%20Christopher&lastname=Roque",headers=headers)



