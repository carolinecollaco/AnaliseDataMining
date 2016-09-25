package analisedatamining;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class FazDataMinings {
    
    public static void fazDataMinings(Connection conexao) throws SQLException{
        float media = pegaMedia(conexao);
        float desvioPadrao = pegaDesvioPadrao(conexao);
        System.out.println("Média: "+media);
        System.out.println("Desvio Padrão: "+desvioPadrao);
    }
    
    public static float pegaMedia(Connection conexao) throws SQLException{
        String query = "SELECT AVG(nota) AS media FROM Curtidas";
        Statement pegaMediaDeRating = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = pegaMediaDeRating.executeQuery(query);
        float media = 0;
        while (resultadoDoSelectNaBase.next()) {            
            media = resultadoDoSelectNaBase.getFloat("media");
        }
        pegaMediaDeRating.close();
        return media;
    }
    
    public static float pegaDesvioPadrao(Connection conexao) throws SQLException{
        String query = "SELECT STDDEV(nota) AS desvio_padrao FROM Curtidas";
        Statement pegaDesvioPadraoDeRating = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = pegaDesvioPadraoDeRating.executeQuery(query);
        float desvioPadrao = 0;
        while (resultadoDoSelectNaBase.next()) {            
            desvioPadrao = resultadoDoSelectNaBase.getFloat("desvio_padrao");
        }
        pegaDesvioPadraoDeRating.close();
        return desvioPadrao;
    }
    
}
