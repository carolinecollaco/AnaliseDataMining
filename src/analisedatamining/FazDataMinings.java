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
        System.out.println("Média: "+media + "\n");
        
        System.out.println("Desvio Padrão: "+desvioPadrao + "\n");
        
        List<String> dezMaioresRatings = pegaDezMaioresRatingsMedios(conexao);
        System.out.println("Dez maiores Ratings Médios ");
        for (String avaliado : dezMaioresRatings) {
            System.out.println(avaliado);
        }    
        List<String> dezMaioresRatingsComMaisDeUmaCurtida = pegaDezMaioresRatingsMediosComMaisDeUmaCurtida(conexao);
        System.out.println("\n Dez maiores Ratings Médios com mair de uma curtida");
        for (String avaliado : dezMaioresRatingsComMaisDeUmaCurtida) {
            System.out.println(avaliado);    
        }    
        
        List<String> dezMaisPopulares = pegaDezMaisPopulares(conexao);
        System.out.println("\n Dez maiores populares");
        for (String avaliado : dezMaisPopulares) {
            System.out.println(avaliado);    
        }  
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
    
    public static List<String> pegaDezMaioresRatingsMedios(Connection conexao) throws SQLException{
        String query = "SELECT avaliado, AVG(nota) AS media_artista FROM Curtidas GROUP BY avaliado ORDER BY media_artista DESC LIMIT 10";
        Statement pegaDezMaioresRatingsMedios = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = pegaDezMaioresRatingsMedios.executeQuery(query);
        List<String> avaliados = new ArrayList<>();
        while (resultadoDoSelectNaBase.next()) {            
            String avaliado = resultadoDoSelectNaBase.getString("avaliado");           
            avaliados.add(avaliado);
        }
        pegaDezMaioresRatingsMedios.close();
        return avaliados;
    }
    
    public static List<String> pegaDezMaioresRatingsMediosComMaisDeUmaCurtida(Connection conexao) throws SQLException{
        String query = "SELECT avaliado FROM (SELECT avaliado, COUNT (nota) AS qntd_curtidas, AVG(NOTA) AS media FROM Curtidas GROUP BY avaliado ORDER BY media DESC ) AS x WHERE qntd_curtidas >=2 LIMIT 10";
        Statement pegaDezMaioresRatingsMediosComMaisDeUmaCurtida = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = pegaDezMaioresRatingsMediosComMaisDeUmaCurtida.executeQuery(query);
        List<String> avaliados = new ArrayList<>();
        while (resultadoDoSelectNaBase.next()) {            
            String avaliado = resultadoDoSelectNaBase.getString("avaliado");           
            avaliados.add(avaliado);
        }
        pegaDezMaioresRatingsMediosComMaisDeUmaCurtida.close();
        return avaliados;
    }
    
        public static List<String> pegaDezMaisPopulares(Connection conexao) throws SQLException{
        String query = "SELECT avaliado, COUNT (nota) AS qntd_curtidas FROM Curtidas GROUP BY avaliado ORDER BY qntd_curtidas DESC LIMIT 10";
        Statement pegaDezMaisPopulares = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = pegaDezMaisPopulares.executeQuery(query);
        List<String> avaliados = new ArrayList<>();
        while (resultadoDoSelectNaBase.next()) {            
            String avaliado = resultadoDoSelectNaBase.getString("avaliado");           
            avaliados.add(avaliado);
        }
        pegaDezMaisPopulares.close();
        return avaliados;
    }
}
