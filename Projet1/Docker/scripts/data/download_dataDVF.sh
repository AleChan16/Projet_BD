set -e

# ==== Configuration ====

DATA_DIR="./data/raw"
DVF_DIR="${DATA_DIR}/dvf"
INSEE_DIR="${DATA_DIR}/insee"

S3_ALIAS="mysdfs"
S3_DVF_PREFIX="raw-data/dvf"
S3_INSEE_PREFIX="raw-data/insee"

# URLs des 5 fichiers DVF: chaque fichier correspond à une année (2020, 2021, 2022, 2023, 2024)
# Source: data.gouv.fr - dernière mise à jour: avril 2026

declare -A DVF_FILES=(
    ["dvf_2024.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
    ["dvf_2023.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/99a26050-b94f-4ffc-9eb0-73ed28a895d1"
    ["dvf_2022.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/025b9d29-8efb-40bb-8ce6-5bddf97a4e51"
    ["dvf_2021.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/be6e092d-292a-4568-90bf-4254a261ff3b"
    ["dvf_2020.txt.zip"]="https://www.data.gouv.fr/api/1/datasets/r/947677ab-ad21-48f4-a9ac-ad217c99cf39"
)

# ==== Vérifications préalables ====

# Installer mc s'il ne l'est pas
if [ ! -f "$HOME/minio-binaries/mc" ]; then
    curl https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs  -o $HOME/minio-binaries/mc
    chmod +x $HOME/minio-binaries/mc
fi
export PATH=$PATH:$HOME/minio-binaries/
mc --version

# Vérifier la bonne configuration de l'alias S3
if mc alias list 2>/dev/null | grep -qw "${S3_ALIAS}"; then
    ok "Alias S3 '${S3_ALIAS}' configuré"
else
    fail "Impossible de configurer l'alias S3. Vérifiez que SeaweedFS est démarré."
    exit 1
fi

# Vérifier que le bucket raw-data a été crée
if ! mc ls ${S3_ALIAS}/raw-data &>/dev/null; then
    info "Bucket raw-data absent, création en cours..."
    mc mb ${S3_ALIAS}/raw-data
    ok "Bucket raw-data créé"
else
    ok "Bucket raw-data disponible"
fi

# Créer les répertoires locaux
mkdir -p "${DVF_DIR}" "${INSEE_DIR}"
ok "Répertoires locaux créés: ${DVF_DIR}, ${INSEE_DIR}"

# ==== Téléchargement des fichiers DVF ====

section "Téléchargement des fichiers DVF (5 ans)"
 
info "Taille estimée: ~360 Mo compressés, ~1.5 Go décompressés"
info "Cela peut prendre plusieurs minutes selon votre connexion..."
echo ""

DVF_SUCCESS=0
DVF_SKIP=0
DVF_FAIL=0

for FILENAME in "${!DVF_FILES[@]}"; do
    URL="${DVF_FILES[$FILENAME]}"
    FILEPATH="${DVF_DIR}/${FILENAME}"
    TXT_FILE="${FILEPATH%.zip}"
 
    # Vérifier si déjà téléchargé et décompressé
    if [ -f "${TXT_FILE}" ]; then
        WARN "${FILENAME%.zip} déjà présent, téléchargement ignoré."
        DVF_SKIP=$((DVF_SKIP + 1))
        continue
    fi
 
    info "Téléchargement de ${FILENAME}..."
    if wget -q --show-progress -O "${FILEPATH}" "${URL}" 2>&1; then
        ok "Téléchargé: ${FILENAME} ($(du -sh "${FILEPATH}" | cut -f1))"
 
        info "Décompression de ${FILENAME}..."
        if unzip -q -o "${FILEPATH}" -d "${DVF_DIR}"; then
            rm "${FILEPATH}"
            # Renommer le fichier extrait avec le nom de l'année
            EXTRACTED=$(ls "${DVF_DIR}"/*.txt 2>/dev/null | head -1)
            if [ -n "${EXTRACTED}" ] && [ "${EXTRACTED}" != "${TXT_FILE}" ]; then
                mv "${EXTRACTED}" "${TXT_FILE}"
            fi
            ok "Décompressé: ${FILENAME%.zip}"
            DVF_SUCCESS=$((DVF_SUCCESS + 1))
        else
            fail "Erreur lors de la décompression de ${FILENAME}"
            DVF_FAIL=$((DVF_FAIL + 1))
        fi
    else
        fail "Erreur lors du téléchargement de ${FILENAME}"
        DVF_FAIL=$((DVF_FAIL + 1))
    fi
done
 
echo ""
info "DVF — Résumé: ${DVF_SUCCESS} téléchargés, ${DVF_SKIP} ignorés, ${DVF_FAIL} erreurs"



