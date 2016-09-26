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
        
        List<String> dezMaioresVariabilidades = pegaDezMaioresVariabilidades(conexao);
        System.out.println("\n Dez maiores variabilidades");
        for (String avaliado : dezMaioresVariabilidades) {
            System.out.println(avaliado);    
        }  
        
        List<String> compartilhamCurtidas = conhecidosQueCompartilhamCurtidas(conexao);
        System.out.println("\n Compartilham o maior numero de artistas curtidos compartilhados");
        for (String pessoa : compartilhamCurtidas) {
            System.out.println(pessoa);    
        }  
        
        pessoasQueCurtiramXArtistas(conexao);
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
        public static List<String> pegaDezMaioresVariabilidades(Connection conexao) throws SQLException{
        String query = "SELECT avaliado FROM (SELECT avaliado, AVG(nota) AS variabilidade_artista, COUNT(nota) AS qntd_curtidas\n" +
        "FROM Curtidas \n" +
        "GROUP BY avaliado \n" +
        "ORDER BY variabilidade_artista DESC) AS Y WHERE qntd_curtidas >=2 LIMIT 10";
        Statement pegaDezMaioresVariabilidades = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = pegaDezMaioresVariabilidades.executeQuery(query);
        List<String> avaliados = new ArrayList<>();
        while (resultadoDoSelectNaBase.next()) {            
            String avaliado = resultadoDoSelectNaBase.getString("avaliado");           
            avaliados.add(avaliado);
        }
        pegaDezMaioresVariabilidades.close();
        return avaliados;
    }
        
    public static List<String> conhecidosQueCompartilhamCurtidas(Connection conexao) throws SQLException{
        String query = "SELECT pessoa, conhecido, COUNT(avaliado) as qnt_iguais\n" +
        "from Conhecidos as conhec, Curtidas as curtidas \n" +
        "where curtidas.avaliador = conhec.pessoa AND EXISTS (\n" +
        "	Select * from Curtidas as ccc, Conhecidos as con\n" +
        "	  where ccc.avaliador = conhec.conhecido \n" +
        "	    and ccc.avaliado = curtidas.avaliado\n" +
        "	    and con.pessoa =  conhec.conhecido\n" +
        "	    and con.conhecido =  conhec.pessoa\n" +
        ")\n" +
        "GROUP BY pessoa, conhecido\n" +
        "ORDER BY qnt_iguais DESC LIMIT 2";
        Statement conhecidosQueCompartilhamCurtidas = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = conhecidosQueCompartilhamCurtidas.executeQuery(query);
        List<String> pessoas = new ArrayList<>();
        while (resultadoDoSelectNaBase.next()) {            
            String pessoa = resultadoDoSelectNaBase.getString("pessoa");           
            pessoas.add(pessoa);
        }
        conhecidosQueCompartilhamCurtidas.close();
        return pessoas;
    }
        
    public static void pessoasQueCurtiramXArtistas(Connection conexao) throws SQLException{
        String query = "Select qnt_avl, count(qnt_avl) as qnt from (SELECT  COUNT(avaliado) AS qnt_avl FROM curtidas GROUP BY avaliador) as  x GROUP BY qnt_avl ORDER BY qnt_avl";
        Statement pessoasQueCurtiramXArtistas = conexao.createStatement();
        ResultSet resultadoDoSelectNaBase = pessoasQueCurtiramXArtistas.executeQuery(query);
        System.out.println("\n f(x) = número de pessoas que curtiram exatamente x artistas");
        System.out.println("...  1  2  3  4  5  6  7");
        while (resultadoDoSelectNaBase.next()) {            
            int qnt_avl = resultadoDoSelectNaBase.getInt("qnt_avl");
            int qnt = resultadoDoSelectNaBase.getInt("qnt"); 
            String eixoX = String.format("%03d ", qnt_avl);
            for (int i = 0; i < qnt; i++) {
                eixoX+= " x ";
            }
            System.out.println(eixoX);
        }
        pessoasQueCurtiramXArtistas.close();
    }
}
