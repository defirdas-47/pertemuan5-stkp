import time
import random


class QuorumNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.storage = {}
        self.version = {}
        self.is_alive = True

    def write_replica(self, key, val, ver):

        if not self.is_alive:
            return False                # Jika node mati penulisan ditolak
        
        self.storage[key] = val              
        self.version[key] = ver         
        
        return True

    def read_replica(self, key):

        if not self.is_alive:
            return None, -1             # Jika node mati, kembalikan nilai kosong

        # Mengembalikan data dan versinya, default 0 jika key belum ada
        return self.storage.get(key, None), self.version.get(key, 0)


# Inisialisasi Cluster & Validasi Aturan Gifford
class QuorumCluster:
    def __init__(self, total_nodes=5, n_read=3, n_write=3):
        self.total_nodes = total_nodes
        self.n_read = n_read
        self.n_write = n_write

        # Buat list berisi 5 instance QuorumNode
        self.nodes = [QuorumNode(i) for i in range(total_nodes)]
        self._validate_quorum_rules()

    def _validate_quorum_rules(self):
        rule1 = (self.n_read + self.n_write) > self.total_nodes         # N_R + N_W > N (3+3 > 5 -> True)
        rule2 = self.n_write > (self.total_nodes / 2)                   # N_W > N/2 (3 > 2.5 -> True)
        
        print(f"=== CLUSTER QUORUM INITIALIZATION ===")
        print(f"Total Nodes (N): {self.total_nodes} | N_R: {self.n_read} | N_W: {self.n_write}")
        print(f"Rule 1 (N_R + N_W > N) -> {rule1} (Read-Write Overlap Terjamin)")
        print(f"Rule 2 (N_W > N/2) -> {rule2} (Write-Write Overlap / Anti-Split-Brain Terjamin)")
        
        if not (rule1 and rule2):
            raise ValueError("Konfigurasi Quorum melanggar aturan Gifford!")

    # Eksekusi Penulisan Klien
    def client_write(self, key, value, active_partition_nodes=None):

        print(f"\n[CLIENT WRITE] Inisiasi Write: {key} = '{value}'")

        # Batasi node yang bisa diakses bila partisi jaringan aktif
        if active_partition_nodes is not None:
            alive_nodes = [node for node in active_partition_nodes if node.is_alive]
        else:
            alive_nodes = [node for node in self.nodes if node.is_alive]

        # cek syara Write Quorum
        if len(alive_nodes) < self.n_write:
            print(f"[WRITE REJECTED] Node aktif ({len(alive_nodes)}) kurang dari N_W ({self.n_write})!")
            return False                    # Ditolak jika node hidup < N_W (3 node)
        
        # Pilih Write Quorum secara acak dari node yang hidup
        write_quorum = random.sample(alive_nodes, self.n_write)

        # Temukan versi tertinggi saat ini di antara node quorum
        max_ver = 0
        for node in write_quorum:
            _, ver = node.read_replica(key)

            if ver > max_ver:
                max_ver = ver

        # Naikan versi data (versi_baru = versi_tertinggi + 1)
        new_ver = max_ver + 1

        # Tulis data dan versi baru ke seluruh node anggota write quorum
        written_ids = []
        for node in write_quorum:
            if node.write_replica(key, value, new_ver):
                written_ids.append(node.node_id)
            
        print(f"[WRITE SUCCESS] Ditulis ke {len(written_ids)} node kuorum: {written_ids} (Version: {new_ver})")
        return True

    # Eksekusi Pembacaan Klien
    def client_read(self, key):

        print(f"\n[CLIENT READ] Inisiasi Read untuk key: '{key}'")

        # Filter node yang hidup
        alive_nodes = [node for node in self.nodes if node.is_alive]

        # Cek syarat read quorum
        if len(alive_nodes) < self.n_read:
            print(f"[READ REJECTED] Node aktif ({len(alive_nodes)}) kurang dari N_R ({self.n_read})!")
            return None
        
        # Pilih Read Quorum secara acak dari node yang hidup
        read_quorum = random.sample(alive_nodes, self.n_read)
        
        latest_val = None
        latest_ver = -1
        sampled_data = []

        # Ambil sampel data dari 3 node kuorum
        for node in read_quorum:
            val, ver = node.read_replica(key)
            sampled_data.append((node, val, ver))

            # Cari data yang punya versi tertinggi
            if ver > latest_ver:
                latest_ver = ver
                latest_val = val
            
        quorum_ids = [node.node_id for node in read_quorum]

        log_samples = [(node.node_id, val, ver) for node, val, ver in sampled_data]
        
        print(f"[READ SAMPLES] Node Kuorum {quorum_ids}: {log_samples}")

        # Kembalikan nilai data dengan versi tertinggi
        print(f"[READ RESULT] Nilai Terkini: '{latest_val}' (Version Tertinggi: {latest_ver})")

        # FITUR READ-REPAIR 
        # Pastiin apakah ada node dalam quorum yang versinya tertinggal
        stale_nodes = [node for node, val, ver in sampled_data if ver < latest_ver]
        if stale_nodes:
            print(f"[READ-REPAIR TRIGGERED] Memperbarui data lama pada node quorum.")
            for node in stale_nodes:
                node.write_replica(key, latest_val, latest_ver)
                print(f"     -> Node-{node.node_id} berhasil di-repair ke Version {latest_ver}")

        return latest_val


def run_quorum_demo():
    cluster = QuorumCluster(total_nodes=5, n_read=3, n_write=3)
    
    # Write awal 
    cluster.client_write("saldo_rek", 5000000)

    # FITUR SIMULASI NETWORK PARTITION (SPLIT-BRAIN PREVENTION)
    print("\n" + "="*50)
    print(">>> SIMULASI NETWORK PARTITION <<<")
    print("Jaringan terbelah jadi Partisi A (Node 0,1,2) & Partisi B (Node 3,4)")
    print("="*50)

    partisi_A = [cluster.nodes[0], cluster.nodes[1], cluster.nodes[2]] # 3 node
    partisi_B = [cluster.nodes[3], cluster.nodes[4]]                   # 2 node

    print("\n--- Uji Write di Partisi A (3 Node >= N_W=3) ---")
    cluster.client_write("saldo_rek", 7000000, active_partition_nodes=partisi_A)

    print("\n--- Uji Write di Partisi B (2 Node < N_W=3) ---")
    cluster.client_write("saldo_rek", 9000000, active_partition_nodes=partisi_B)

    # Langkah 3: Matikan 2 node (Node-0 dan Node-1, Sisa 3 Node Aktif)
    print("\n>>> Skenario: Node-0 dan Node-1 down (Sisa 3 node aktif) <<<")
    cluster.nodes[0].is_alive = False
    cluster.nodes[1].is_alive = False

    # Langkah 4: Operasi Write baru dengan 3 node yang masih hidup (masih Sukses karena 3 node aktif >= N_W & N_R)
    cluster.client_write("saldo_rek", 7500000)
    
    # Langkah 5: Read kembali (pembuktian konsistensi data terbaru tetap terbaca)
    cluster.client_read("saldo_rek")
    
    # Langkah 6: Matikan 1 node lagi (sisa 2 node aktif)
    print("\n>>> Skenario: Node-2 down (Sisa 2 node aktif) <<<")
    cluster.nodes[2].is_alive = False
    
    # Langkah 7: Uji Write dan Read saat kuorum tidak terpenuhi (gagal karena 2 node aktif < N_W=3 dan N_R=3)
    cluster.client_write("saldo_rek", 9000000)
    cluster.client_read("saldo_rek")

if __name__ == '__main__':
    run_quorum_demo()


