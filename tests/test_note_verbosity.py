"""Regresion del conversor con volcados de MariaDB 12 para MariaDB 10."""
from pathlib import Path
import subprocess
import tempfile
import unittest


EXE = Path(__file__).resolve().parents[1] / 'DBComparer.exe'


class ConversionNoteVerbosity(unittest.TestCase):
    def test_guardas_y_literales(self):
        origen = (
            'SET @OLD_NOTE_VERBOSITY=@@NOTE_VERBOSITY, NOTE_VERBOSITY=0;\n'
            'CREATE TABLE prueba (ID int, TEXTO varchar(200));\n'
            "INSERT INTO prueba VALUES (1, 'SET NOTE_VERBOSITY=0');\n"
            'DELIMITER ;;\n'
            'CREATE PROCEDURE PRC_PRUEBA()\n'
            "BEGIN SELECT 'NOTE_VERBOSITY' AS TEXTO; END;;\n"
            'DELIMITER ;\n'
            'SET @OTRO=7;\n'
            'SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY;\n'
        )
        with tempfile.TemporaryDirectory(prefix='dbcomparer_note_') as carpeta:
            entrada = Path(carpeta) / 'entrada.sql'
            salida = Path(carpeta) / 'salida.sql'
            entrada.write_text(origen, encoding='utf-8')
            for dialecto in ['mariadb10', 'mariadb12']:
                proceso = subprocess.run(
                    [str(EXE), '--convertir', str(entrada), str(salida),
                     '--destino=' + dialecto], capture_output=True)
                self.assertEqual(proceso.returncode, 0, proceso.stderr)
                texto = salida.read_text(encoding='utf-8-sig')
                self.assertIn("INSERT INTO prueba VALUES (1, 'SET NOTE_VERBOSITY=0')", texto)
                self.assertIn("BEGIN SELECT 'NOTE_VERBOSITY' AS TEXTO; END;;", texto)
                self.assertIn('SET @OTRO=7;', texto)
                self.assertEqual(texto.count('/*M!100616'),
                                 2 if dialecto == 'mariadb10' else 0)


if __name__ == '__main__':
    unittest.main()
