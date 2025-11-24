export interface FuturamaCharacter {
  id: number;
  name: {
    first: string;
    middle?: string;
    last: string;
    full: string;
  };
  age?: string;
  images: {
    headShot?: string;
    main?: string;
  };
  gender?: string;
  species?: string;
  homePlanet?: string;
  occupation?: string;
  sayings?: string[];
  createdAt?: string;
  updatedAt?: string;
}

