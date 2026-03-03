from django.test import SimpleTestCase

from publications import services


class EncodeProcessingExtractionTests(SimpleTestCase):
    def test_extract_encode_processing_steps_uses_detail_and_files(self):
        exp = {
            "accession": "ENCSR773ABC",
            "assay_title": "eCLIP",
            "description": "Reads were processed with the ENCODE eCLIP pipeline.",
            "biosample_summary": "K562",
        }
        detail = {
            "notes": ["Peak calling and IDR filtering were applied."],
        }
        files = [
            {
                "mapped_by": "STAR",
                "output_type": "alignments",
                "assembly": "hg38",
                "genome_annotation": "GENCODE v29",
                "file_format": "bam",
            }
        ]

        steps, raw_text = services._extract_encode_processing_steps(exp, detail=detail, files=files)

        self.assertGreaterEqual(len(steps), 3)
        self.assertIn("STAR", raw_text)
        self.assertIn("alignments", raw_text)
        self.assertIn("hg38", raw_text)

    def test_extract_encode_processing_steps_has_fallback(self):
        exp = {"accession": "ENCSR000AAA", "assay_title": "", "description": ""}
        steps, raw_text = services._extract_encode_processing_steps(exp, detail={}, files=[])

        self.assertEqual(len(steps), 1)
        self.assertIn("ENCSR000AAA", raw_text)
