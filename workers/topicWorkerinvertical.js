// workers/topicWorkerinvertical.js
require('dotenv').config();
const { supabase } = require('../config/supabaseClient');
const openai = require('../config/openaiClient');

// ---------- Settings ----------
const TOPIC_MODEL         = process.env.TOPIC_MODEL || "gpt-5-mini";
const TOPIC_LIMIT         = parseInt(process.env.TOPIC_LIMIT || "180", 10);
const TOPIC_BLOCK_SIZE    = parseInt(process.env.TOPIC_BLOCK_SIZE || "60", 10);
const TOPIC_SLEEP_MS      = parseInt(process.env.TOPIC_LOOP_SLEEP_MS || "800", 10);
const TOPIC_LOCK_TTL_MIN  = parseInt(process.env.TOPIC_LOCK_TTL_MIN || "15", 10);
const WORKER_ID           = process.env.WORKER_ID || `topic-${process.pid}-${Math.random().toString(36).slice(2,8)}`;

// ---------- Topic List ----------
const TOPICS = [
  "Vitamins", "DNA", "Amino Acid", "Lipoproteins", "Protein Structure", "RNA", "Citric Acid Cycle", "Glycolysis", "Electron Transport Chain", "Enzyme Kinetics", "Chromatography", "Hemoglobin", "Collagen Elastin", "Fatty Acid Synthesis  Laproscopy",Laproscopy", "Pelvis Diametres", "Trial Labour", "TrichomonasRupture", "UTI", "CAH", "Cervical Erosion", "Cord Prolapse", "ECV", "Gartner Cyst", "Gonococcus", "Gynaecomastiaormone", "Insulin", "PCR", "Purine Metabolism", "Serine Protease", "Carbohydrate Digestion", "EFA", "Enzyme Classification", "Galactosemia", "Glycogen Storage Disorder", "Glycogen Synthesis", "Starvation", "Glutathione", "Glycogenolysis", "Ketone Bodies", "Nitric Oxide", "Phenylketonuria", "Xeroderma Pigmentosa", "Chaperones", "Chromosome Types", "Cytochrome P 450", "Diffusion", "Fructose Metabolism", "GIT Enzymes", "Malate Shuttle", "Porphyria", "Portal Vein", "Abdominal Aorta Branches", "Inguinal Canal", "Inguinal Lymph Node", "IVC", "Kidney", "Spleen", "Coeliac Trunk", "Liver Lobes", "Anal Canal", "Gut Blood Supply", "Lesser Sac", "Mesentric Vein", "Stomach", "Suprarenal Gland", "Branchial Arch Derivatives", "Ecto derivatives", "Endo derivatives", "Mesoderm derivatives", "Genitourinary Embryology", "Neural Crest", "Intervertebral Disc", "Joint Types", "Vertabra", "Ossification", "Epithelial Lining", "Histology", "Mitosis Meiosis", "Carotid Artery", "Vocal Cord Muscles and Nerves", "Cavernous Sinus", "Ocular Muscle", "Parotid", "Palate", "Superior Orbital Fissure", "Temporomandibular Joint", "Saphenous Vein", "Ankle Joint", "Femoral Artery", "Knee Joint", "Obturator Nerve", "Surface Markings", "Facial Nerve", "Root Value", "Cerebral Artery", "Trigeminal Nerve", "Auditory Pathway", "Spinal Cord", "Accessory Nerve", "Brainstem", "Cerebral Venous Sinus", "Oculomotor Nerve", "Trochlear Nerve", "Ureter", "Perineal Pouch", "Epididymis", "Cremasteric", "Penis", "Prostate Anatomy", "Urethra", "Urinary Bladder", "Uterus", "Streptococcus", "HIV", "Chlamydia", "Cholera", "Immunoglobulin", "E Coli", "DNA Viruses", "Staphylococcus", "Diptheria", "Tuberculosis", "Cryptococcus", "Clostridium", "Helicobacter", "Malaria", "EBV", "Sterilisation", "Anthrax", "HBV", "Rickettsia", "Syphylis", "Typhoid", "Giardiasis", "Immunodeficiency Disorders", "Rota Virus", "Toxoplasma", "Agglutination Precipitation", "Hypersensitivity", "Leprosy", "Maduramycosis", "Rabies", "RNA Viruses", "Amebiasis", "Bacteroides", "Cysticercosis", "Filariasis", "Gonococcus", "Mycoplasma", "Prion", "Brucella", "Dermatophyte", "Influenza", "Legionella", "Leptospira", "Drug Resistance Mutation", "PCR", "Shigella", "Ankylostoma", "Aspergillus", "Dengue", "DNAfingerprinting", "Drug Resistance", "Fungi Classification", "Herpes Zoster", "Infective Endocarditis", "Interluekin", "Pnuemocystis", "Proteus", "Reverse transcriptase", "RSV", "Salmonella", "Pharmacokinetics", "Drug Toxicity", "Asthmatc Drugs", "Beta Blocker", "ATT", "ACE Inhibitors", "ANS Pharmacology", "Digitalis", "HAART", "Aspirin", "Diuretics", "Glucocorticoids", "Antiarrythmics", "Parkinsonism Drugs", "Pencillin", "Pregnancy", "Opioids", "P450", "Malaria Drugs", "Ionotropic Agents", "Psuedomonas Drugs", "SSRI", "Floroquinolones", "Phenytoin", "Aminoglycoside", "Antiemetics", "Heparin", "Hypolipidemic Drugs", "Methotrexate", "Renal failure and drugs", "Warfarin", "Antiepileptics", "Antihistamines", "Benzodiazepenes", "Calcium Channel Blockers", "Erythromycin", "Leprosy Drugs", "Muscle Relaxants", "Peptic Ulcer Treatment", "Prostate Drugs", "Adriamycin", "Alkylating Agents", "Cyclosporine", "Lithium", "Nitrates", "Nitroprusside", "Tetracyclines", "Cephalosporin", "Gout Treatment", "Gynaecomastia", "Haloperidol", "Immunosuppressants", "Metformin", "Morphine", "MRSA Drugs", "OCP", "Prazosin", "Pulmonary Fibrosis", "Sulphonamides", "Antifungals", "Glomerlonephritis", "Inflammation", "Amyloidosis", "Apoptosis", "Lymphoma", "Hypertension", "Nephrotic Syndrome", "Tumor Markers", "Hepatitis", "CD Markers", "Tumor Suppressor Genes", "Inheritance Patterns", "Myocardial Infaction", "Blood Group", "Coagulation", "Hypersensitivity", "AML/ALL", "Lung Cancer", "Granuloma", "SLE", "Transplantation", "Vasculitis", "Interleukin", "Multiple Myeloma", "Sickle Cell Anemia", "Calcification", "Jaundice", "MHC", "RHD", "Thalessemia", "Ulcerative Colitis", "Diabetes", "Hemolytic Anemia", "Infective Endocarditis", "ADPKD/ARPKD", "CNS General", "DIC", "Fatty Liver", "GIT Tumors", "HUS", "Hypercoagubility", "Lipoproteins", "Megaloblastic Anemia", "Necrosis", "Neuroblastoma", "Thyroid Cacinoma", "Alport Syndrome", "APC", "Breast Carcinoma", "CML/CLL", "CNS General", "COPD", "Free Radical Injury", "Histiocytosis", "HIV", "Iron Deficiency Anemia", "Stain", "Aortic Anueurysm", "Asbestosis", "Celiac Disese", "Drowning", "Adipocere\Putrfaction\Mummification", "Arsenic Poisning", "Identification", "Organophosphorus Poisoning", "Mercury Poisoning", "Rigor Mortis", "Alcohol", "Asphyxial Death", "Laceration", "Cyanide Poisoning", "Fingerprint", "Atropine Poisoning", "Barbiturate Poisoning", "Cocaine", "Dialysis", "Lead Poisoning", "Postmortem Hypostasis", "Snake Bite", "Blood Stain", "Fire Arm Wound", "Insanity", "Postmortem Caloricity", "Preservatives", "Rape", "Sexual Perversion", "Antemortem vs Postmortem wounds", "Grievous Hurt", "Gun Powder", "IUD", "Opioid Poisoning", "Consent", "Gastric Lavage", "Inquest", "MTP", "Pugilistic Attitude", "Suspended Animation", "Indicators of Health", "Vaccines", "Statastics", "Case Control Cohort", "Sensitivity Specificity", "Tuberculosis", "Malaria", "RDA", "Blindness control", "Chi Square and T test", "Leprosy", "Sentinel Surveilance", "Vectors", "Contraception", "Incidence Prevalence", "Sampling", "Screening", "PHC RHC", "Health Planning", "Non Communicable Disease", "Normal Curve", "Occupational Disorders", "Rabies", "Rickettsiae", "HFA 2000/Vision 2020", "Pollution", "Tetanus", "Demographic Stages", "Measles", "P Value", "Carrier", "Case Fatality Rate", "Chlorination", "HDI PQLI", "Japanese Encephalitis", "Prevention Levels", "Serial Interval Generation Time", "Sociology", "Cholera", "Correlation", "Food Poisoning", "Incubation Period", "ORS", "Polio", "Randomisation", "Relative Risk Attributable Risk", "Yellow Fever", "Nominal Ordinal", "Standardisation", "Dracunculosis", "Point Source Propogated", "Anemia", "Epidemic Dropsy", "ESI", "Fats", "ICDS", "Iodine Deficiency Disorder Control Programme", "Isolation", "MCH", "NPU", "Pertussis", "Trachoma", "Arti", "Cyclopropagative", "Disinfectant", "Filariasis", "Insecticides", "Keratomalacia", "Milk", "Sample Registration System", "Secondary Attack Rate", "Bias", "Blinding", "Diphtheria", "Flourosis", "HBV", "Herd Immunity", "Histogram Bar Diagram", "HIV", "Meningococcal Meningitis", "Obesity", "Plague", "Preterm", "Rapid Sand Filter", "Alpha Error Beta Error", "Cereals and Pulses", "Chemoprophylaxis", "Epidemic Pandemic Definition", "Folate", "Influenza", "Anatomy", "Larynx Carcinoma", "Facial Nerve Palsy", "Otosclerosis", "Acoustic Neuroma", "Cholesteatoma", "CSOM", "Deafness", "Nasopharyngeal Carcinoma", "Meniere Disease", "Nasopharyngeal Angiofibroma", "Vocal Cord Paralysis", "CSF Rhinorrhea", "Epistaxis", "Quinsy", "Sinus Carcinoma", "Tracheostomy", "DNS", "Epiglottitis", "Glomus Tumor", "Laryngitis Pachyderma", "Laryngomalacia", "Malignant Otits Externa", "Mastoidectomy", "Papilloma", "Vocal Nodule", "Glaucoma", "Cataract", "Uveitis", "Conjunctivitis", "Visual Field Defects", "Diabetic Retinopathy", "Retinoblastoma", "Anatomy", "Cranial Nerve Palsy", "Myopia", "Herpes", "Papilledema", "Strabismus", "Corneal ulcer", "Ophthalmoscopy Direct & Indirect", "Blunt Trauma Eye", "Contact Lens", "Trachoma", "CRAO/CRVO", "Sympathetic Ophthalmitis", "Proptosis", "Blindness Control Programme", "Amblyopia", "Dacryocystitis", "Hypermetropia", "Mydriatics", "Optic neuritis", "Chalazion", "Pupil Abnormalities", "Retinitis Pigmentosa", "Colour Blindness", "Horner Syndrome", "Keratomalacia", "Lens Dislocation", "Acanthameba", "Central Serous Retinopathy", "Corneal Dystrophy", "Enucleation Evisceration", "Foreign Body", "Keratoconus", "Melanoma", "Orbit Fracture", "Paras Planitis", "Retinal Detatchment", "Best Disease", "Cavernous Sinus Thrombosis", "CMV Retinitis", "Eales Disease", "Ophthalmia Neonatarum", "Optic Nerve Glioma", "Retrolental Fibroplasia", "Aphakia", "Band Keratopathy", "Internuclear Ophthalmoplegia", "Keratotomy", "Optic Atrophy", "Ptosis", "Rhabdomyosarcoma", "Vitreous Hemorrhage", "Local Anesthetic", "Ketamine", "Succinylcholine", "Spinal Anaesthesia", "Anaesthesia Complications", "Halothane", "Thiopentone", "Paediatric Anesthesia", "Ventilator", "Atracurium", "Intubation", "Muscle Relaxant", "Propofol", "Isoflurane", "Opioids", "Bupivacaine", "Pin Index", "MAC", "Mcgill Circuit", "Sevoflurane", "Airway", "CPR", "Intraoperative Management", "Desflurane", "Muscular Dystrophy/myasthenia", "Preanesthetic Medication", "Air Embolism", "Hypothermia", "Nitric Oxide", "Postoperative Complications", "Tubocurarine", "Vecuronium", "Bone Tumor", "Supracondylar Fracture Humerus", "Avasclar Necrosis", "Shoulder Dislocation", "Dislocation Hip", "Tuberculosis Spine", "Bone Cyst", "Cervical Spine Injuries", "Congenital Dislocation Hip", "Rickets", "Osteomyelitis", "CTEV", "Fracture Neck Femur", "Osteoarthritis", "Humerus Fracture", "Nails Screws Wires", "Osteoporosis", "Colles Fracture", "Cruciate Ligament Injury", "Meniscus Injury", "Bursitis", "Fat Embolism", "Gout", "Lateral Condylar Fracture Humerus", "Poliomyelitis", "Ulnar Nerve", "Ankle Sprain", "Carpal Tunnel Syndrome", "Osteochondritis", "Pyogenic Arthritis", "Amputation", "Ankylosing Spondylitis", "Disc Prolapse", "Dislocation Knee", "Genu Varum", "Hyperparathyroidism", "Osteogenesis Imperfecta", "Paget Disease", "Patella Fracture", "Radial Nerve", "Stress Fracture", "Tibia Fracture", "Calcaneum Fracture", "Charcot Joint", "Elbow Dislocation", "Femur Shaft Fracture", "Perthes Disease", "Trendelenburgh Sign", "Actinomycosis", "Carpometacarpal Fracture", "Clavicle Fracture", "Median Nerve", "Meralgia Paresthetica", "Slipped Capital Femoral Epiphyses", "Tuberculosis Knee", "Achondroplasia", "Compartment Syndrome", "Crush Injury", "Dupytrens Contracture", "Erb Palsy", "Developmental Milestones", "Hypothyroidism", "Jaundice", "Meningitis", "PEM", "Rickets", "Dehydration", "Thalassemia", "Transient Tachypnea of New born (TTN)", "Vescico Ureteric Reflux", "TOF", "Brain Tumor", "Breast Feeding", "Dowm Syndrome", "Neuroblastoma", "Reflexes", "ALL", "Bronchiolitis", "Pnuemonia", "AD/AR/XR/XD Inheritance Pattern", "Asthma", "Biliary Atresia", "CAH", "Epilepsy", "Hbv", "Rubella", "Diaphragmatic Hernia", "Dwarfism", "LGA Nephropathy", "Nephrotic Syndrome", "PDA", "Puberty", "SGA", "Tricuspid Atresia", "Tuberculosis", "Coarctation of Aorta", "Congenital Heart Disease", "Croup", "Hemophilia", "HIV", "Hydrocephalus", "Metabolic Alkalosis", "Nephrolithiasis", "Vaccination", "ASD/VSD", "Diabetic Mother", "Diarrhea", "Glomerulonephritis", "Hypothermia", "Intubation", "ITP", "Juvenile Rheumatoid Arthritis", "Neonatal Sepsis", "Nephronophthisis", "TORCH", "Erythema Infectiosum", "Cephaly", "Febrile Convulsions", "Hypertension", "Normal Newborn", "Schizophrenia", "Depression", "OCD", "Mania", "Delusion", "Dementia", "ECT", "Fluoxetine", "Hallucination", "Phobia", "Alcoholism", "Delirium Tremens", "Personality Disorder", "Hypochondriasis", "Opioid", "PTSD", "Autistic Disorder", "Clozapine", "Haloperidol", "ADHD", "Dissociative Disorder", "Panic", "Conversion", "Defense Mechanism", "Mental Retardation", "Antidepressant", "Conditioning", "Impotence", "Lithium", "Alzhemir Disease", "Amitryptiline", "Amnesia", "Behaviour Therapy", "Cannabis and substance abuse", "Delirium", "Organic Brain Syndrome", "Sleep", "Adjustment Disorder", "Akathasia", "EEG", "Ganser Syndrome", "Impulse Disorder", "Learning Disability", "Sertraline", "Wernicke Korsakoff Psycosis", "Leprosy", "Dermatophyte Infection", "Lichen Planus", "Psoriasis", "Pemphigus", "Syphilis", "Acne", "Tuberculosis Skin", "Alopecia", "Scabies", "Atopic Dermatitis", "Contact Dermatitis", "LGV", "Chancroid", "Dermatitis Herpetiformis", "Pityriasis Rosea", "Tinea Versicolor", "Pityriasis Alba", "Tuberous Sclerosis", "Acanthosis Nigricans", "DLE", "Donovaniosis", "Drug Reaction", "Epidermolysis Bullosa", "Erythema Multiforme", "Gonorrhea", "HSV", "Molluscum Contagiosum", "Urticaria", "Actinitic Keratosis", "Chloasma", "Darier's Disease", "Physics", "Radionuclide", "Radiosensitivity", "Radiation Efect", "Pregnancy Imaging", "Bone Tumors", "Chest x ray", "Contrast Agents", "Intracranial Calcification", "Pancreatitis", "Pulmonary Embolism", "Rickets", "Miliary Shadowing", "Radionuclide Heart", "Renal Tuberculosis", "Silicosis", "SOL Lung", "Breast Carcinoma", "Coarctation of Aorta", "Interstitial Lung Disease", "Meningioma", "MRI", "Neural Tube Defects", "Pericardial Effusion", "Pleural Effusion", "TAPVC", "CHF", "Cholecystitis", "Cranial Irradiation", "Histiocytosis", "Kerley Lines", "Mediastinal Mass", "Mitral Stenosis", "Myocardial Infarction", "Hypertension", "Infective Endocarditis", "Heart Sound", "Coarctatio Of Aorta", "RHD", "Cardiac Tamponade", "HOCM", "ECG", "Pulmonary Embolism", "Pulse", "Digitalis", "Clubbing", "JVP", "CHF", "QT Prolong", "Shock", "Mitral Stenosis", "Miral Regurgitation and MVP", "Aortic Stenosis", "Aortic Regurgitation", "Mitral Valve Prolapse", "Heart Block", "Supraventricular Tachycardias", "Atrial Fibrillation", "Atrial Flutter", "Sustained Ventricular Tachycardia", "Ventricular Tachyarrythmias", "Pulmonary Hypertension", "Atrial Myxoma", "Constrictive Pericarditis", "Diabetes", "Diabetes Complications", "Diabetic Keto Acidosis", "Hypercalcemia and Hyperparathyroidism", "Cushing Syndrome", "Addison Disease", "Conn Syndrome", "MEN Syndrome", "Wilson Disease", "Osteoporosis", "CAH", "Hemochromatosis", "Testicular Feminization Syndrome", "Meningitis", "CVA", "Epilepsy", "Subarachnoid Hemorrhage", "Myasthenia Gravis", "Parkinson Disease", "Peripheral Neuropathy", "Lobar Damage", "Headache", "Lateral Meddullary Syndrome", "Alzhemir Disease", "Neurocysticercosis", "Spinal Cord Compression", "Umn lmn", "Multiple Sclerosis", "Huntinghton Corea", "Gullian Barre Syndrme", "Motor Nueron Disease", "SLE", "Rheumatoid Arthritis", "HS Purpura", "Poly Arteritis Nodosa", "Gout", "Sarcoidosis", "Wegener Granulomatosis", "Systemic Sclerosis", "Vasculitis", "Behcets Disease", "Polymya Rhuematica Giant Cell Arteritis", "Polymyosistis Dermatomyositis", "CRF", "Glomerulonephritis", "Nephrotic Syndrome", "Interstitial Nephritis", "Urine Exam", "Hypercalcemia", "Hyperkalemia", "SIADH", "Renal Tubular Acidosis (RTA)", "Metabolic Acidosis", "Metabolic Alkalosis", "Respiratory Acidosis", "Respiratory Alkalosis", "Hypokalemia", "Hyponatremia", "Hypernatremia", "Asthma", "Pleural Effusion", "Lung Function Tests", "Hemoptysis", "Pneumonia", "ABPA", "ARDS", "Multiple Myeloma", "Lymphoma", "Iron Deficiency Anemia", "Megaloblastic Anemia", "Hemolytic Anemia", "Polycythemia Vera", "HUS", "PNH", "AML", "CML", "Hemophilia", "Transfusion Complications", "Aplastic Anemia", "Thalasemia", "Lung Cancer", "Brain Tumor", "Tumor Marker", "Hepatocellular Carcinoma", "Thymoma", "Tumor Lysis Syndrome", "Paraneoplastic Syndrome", "Breast Carcinoma", "Thyroid Carcinoma", "Prostate Carcinoma/BPH", "Salivary Tumors", "Carcinoma Colon", "Gall Stone", "Peptic Ulcer", "Testicular Carcinoma", "IBD", "Congenital Hypertrophic Pyloric Stenosis", "Gastric Carcinoma", "Splenectomy", "Hernia", "Hepatocellular Carcinoma", "Pancreatitis", "Renal Cell Carcinoma", "Renal Stone", "Thyroidectomy", "Esophageal Carcinoma", "Intestinal Obstruction", "TAO", "Burns", "Grafting", "Intestinal Polyp", "CBD Stone", "Shock", "Blunt Injury Abdomen", "Hirschsprung's Disease", "Barret Esophagus", "Bladder Cancer", "DVT", "Melanoma", "Pancreatic Carcinoma", "Appendicitis", "Av Fistula", "Soft Tissue Sarcoma", "Urethral Rupture", "Achalasia", "Carcinoma Tongue", "Duct Papilloma", "Anal Carcinoma", "Bronchogenic Carcinoma", "Diaphragmatic Hernia", "Rectum", "Total Parenteral Nutrition", "Liver Abscess", "Lymphadenopathy", "Meckels Diverticulum", "Psuedocyst Pancreas", "Varicose Veins", "Billroth Gastectomy", "Epidural Hematoma", "Nueroblastoma", "Peritonitis", "Portal Hypertension", "Carcinoid", "Cholecystitis", "Hypospadias", "Insulinoma", "Mediastinal Tumor", "Mesentry", "Postoperative Patient", "Renal Transplantation", "Tuberculosis", "Basal Cell Carcinoma", "Hemorrhoids", "Parathyroid Adenoma", "Pheochromocytoma", "Aortic Anuerysm", "Carotid Body Tumor", "Cystic Hygroma", "Intusussception", "Liver Transplantation", "Marjolin Ulcer", "Meconium Ileus", "Oral Cancer", "Posterior Urethral Valve", "Reflux Esophagitis", "Sutures", "Thyroiditis", "Volvulus", "Wound Infection", "Cleft Lip", "Compartment Syndrome", "Diverticulosis", "Hemangioma", "Ischemic Bowel Disease", "MEN", "ZenkerDiverticulum", "Annular Pancreas", "Carcinoma Gallbladder", "Salivary Calculus", "Contraception", "Ovarian Tumor", "Carcinoma Cervix", "Infertility", "Diabetic Mother", "Ectopic Pregnancy", "Gestational Trophoblastic Tumor", "Fibroid Uterus", "Abnormal Presentation", "Systemic Conditions and Pregnancy", "Abortion", "Endometrial Carcinoma", "Menstrual Irregularities", "Preeclampsia", "Down Syndrome", "Ovulation", "Heart Disease in Pregnancy", "Abruptio Placenta", "Torch", "AFP", "Physiologic Changes in Pregnancy", "Placenta Previa", "Preterm Labour", "Rh Isoimmunisation", "Nueral Tube Defects", "Polyhydramnios", "Prolapse", "Tuberculosis", "Chorionic Villus Sampling", "IUGR", "PCOD", "Puerperium", "PPH", "Ultrasound in Pregnancy", "Endometriosis", "Breast feeding", "Endometrial Hyperplasia", "Induction of Labour", "Testicular Feminisation Syndrome", "Anatomy", "Bacterial Vaginosis", "Instrumental Delivery", "Mullerian Anomalies", "NST", "Postcoital Contraception", "Twins", "Anemia", "Rokitansky Kustner Hauser Syndrome", "Colposcopy", "Hormone Replacement Therapy", "Menopause", "Pelvis Types", "PID", "Puberty", "Retroverted Uterus", "Tocography", "VVF", "Intrauterine Death", "Pap Smear", "Pregnancy Signs", "Turner Syndrome", "Antiphospholipid Antibody Syndrome", "Candidiasis", "Cesarean Section", "Condyloma Accuminata", "HIV", "Hysteroscopy", "Pituitary Ademoma", "Placenta", "Postterm Pregnancy", "Pyometra", "Carcinoma Vulva", "chlamydia", "Fetal Hypoxia", "Fetal Skull", "HCG", "Incontinence", "Laproscopy", "Pelvis Diametres", "Trial Labour", "Trichomonas", "Uterine Rupture", "UTI", "CAH", "Cervical Erosion", "Cord Prolapse", "ECV", "Gartner Cyst", "Gonococcus", "Gynaecomastia", "Hydrocephalus", "Hysterectomy", "Sheehan's Syndrome"
];

// ---------- Helpers ----------
function extractStem(mcqJson) {
  if (!mcqJson) return '';
  if (typeof mcqJson === 'string') return mcqJson;
  if (typeof mcqJson === 'object') return mcqJson.stem || mcqJson.question || mcqJson.text || JSON.stringify(mcqJson);
  return String(mcqJson);
}

const truncate = (s, n = 600) =>
  (String(s || '').length > n ? String(s).slice(0, n) + ' …' : String(s || ''));

function buildPrompt(items) {
  const header = `
You are an expert medical teacher. Classify each MCQ into EXACTLY one topic.

Use ONLY these exact topics (no synonyms, no new topics):
${TOPICS.map(t => `- ${t}`).join('\n')}

Return format:
- Output EXACTLY ${items.length} LINES.
- Each line = one topic (from the list above), in the same order as the MCQs.
- No numbering, no extra words.
`.trim();

  const body = items.map((it, i) =>
    `${i + 1}) ${truncate(extractStem(it.mcq_json))}`).join('\n\n');

  return `${header}\n\nMCQs:\n\n${body}\n\nRemember: output exactly ${items.length} lines, one topic per line.`;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isRetryable(e) {
  const s = String(e?.message || e);
  return /timeout|ETIMEDOUT|429|rate limit|temporar|unavailable|ECONNRESET/i.test(s);
}

async function callOpenAI(messages, attempt = 1) {
  try {
    const resp = await openai.chat.completions.create({
      model: TOPIC_MODEL,
      messages
    });
    return resp.choices?.[0]?.message?.content || '';
  } catch (e) {
    if (isRetryable(e) && attempt <= 3) {
      await sleep(400 * attempt);
      return callOpenAI(messages, attempt + 1);
    }
    throw e;
  }
}

// ---------- Locking & Claim ----------
async function claimRows(limit) {
  const cutoff = new Date(Date.now() - TOPIC_LOCK_TTL_MIN * 60 * 1000).toISOString();

  // free stale locks (only for unclassified)
  await supabase
    .from('learning_gap_vertical')
    .update({ chapter_lock: null, chapter_lock_at: null })
    .is('chapter', null)
    .lt('chapter_lock_at', cutoff);

  const { data: candidates, error: e1 } = await supabase
    .from('learning_gap_vertical')
    .select('id, mcq_json')
    .is('chapter', null)
    .order('id', { ascending: true })
    .limit(limit);
  if (e1) throw e1;
  if (!candidates?.length) return [];

  const ids = candidates.map(r => r.id);

  const { data: locked, error: e2 } = await supabase
    .from('learning_gap_vertical')
    .update({ chapter_lock: WORKER_ID, chapter_lock_at: new Date().toISOString() })
    .in('id', ids)
    .is('chapter', null)
    .is('chapter_lock', null)
    .select('id, mcq_json');
  if (e2) throw e2;

  console.log(`🔎 candidates=${candidates.length}, locked=${locked?.length}`);
  return (locked || []).slice(0, limit);
}

async function clearLocks(ids) {
  if (!ids.length) return;
  await supabase
    .from('learning_gap_vertical')
    .update({ chapter_lock: null, chapter_lock_at: null })
    .in('id', ids);
}

// ---------- Process one block ----------
async function processBlock(block) {
  const prompt = buildPrompt(block);
  const raw = await callOpenAI([{ role: 'user', content: prompt }]);

  const lines = (raw || '')
    .trim()
    .replace(/^```.*?\n|\n```$/g, '')
    .split(/\r?\n/)
    .map(l => l.replace(/^\d+[\).\s-]+/, '').trim())
    .filter(Boolean);

  const updates = [];
  for (let i = 0; i < block.length && i < lines.length; i++) {
    const topic = TOPICS.find(t => t.toLowerCase() === lines[i].toLowerCase());
    if (topic) {
      updates.push({ id: block[i].id, chapter: topic });
    }
  }

  if (updates.length) {
    for (const u of updates) {
      const { error: upErr } = await supabase
        .from('learning_gap_vertical')
        .update({ chapter: u.chapter })
        .eq('id', u.id);
      if (upErr) throw upErr;
    }
  }

  await clearLocks(block.map(r => r.id));

  return { updated: updates.length, total: block.length };
}

// ---------- Main Loop ----------
(async function main() {
  console.log(`🧵 Topic Worker ${WORKER_ID} | model=${TOPIC_MODEL} | claim=${TOPIC_LIMIT} | block=${TOPIC_BLOCK_SIZE}`);

  while (true) {
    try {
      const claimed = await claimRows(TOPIC_LIMIT);
      if (!claimed.length) {
        await sleep(TOPIC_SLEEP_MS);
        continue;
      }

      console.log(`⚙️ claimed=${claimed.length}`);
      let updated = 0;
      for (let i = 0; i < claimed.length; i += TOPIC_BLOCK_SIZE) {
        const block = claimed.slice(i, i + TOPIC_BLOCK_SIZE);
        try {
          const r = await processBlock(block);
          updated += r.updated;
          console.log(`   block ${i / TOPIC_BLOCK_SIZE + 1}: updated ${r.updated}/${r.total}`);
        } catch (e) {
          console.error('   block error:', e.message || e);
          await clearLocks(block.map(r => r.id));
        }
      }

      console.log(`✅ loop updated=${updated} of ${claimed.length}`);
    } catch (e) {
      console.error('Loop error:', e.message || e);
      await sleep(1000);
    }
  }
})();
